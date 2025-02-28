import { Injectable } from '@nestjs/common';
import { DataSource, DataSourceOptions } from 'typeorm';
import { mysqlConfig, postgresConfig } from './database.config';
import * as fs from 'fs';
import * as path from 'path';

function formatValue(value: any, type: string): string {
  if (value === null || value === undefined || value === "null") {
    if (type === 'timestamp' || type === 'datetime') {
      return 'NULL';
    }
    return 'NULL';
  }

  if (type === 'timestamp' || type === 'datetime') {
    try {
      const date = new Date(value);
      if (!isNaN(date.valueOf())) {
        // UTC formatida saqlash
        return `'${date.toISOString().slice(0, 19).replace('T', ' ')}'`;
      }
      return 'NULL';
    } catch (err) {
      console.error(`Error parsing date: ${value}`, err);
      return 'NULL';
    }
  } else if (type === 'text' || type === 'varchar' || type === 'longtext') {
    return `'${value.toString().replace(/'/g, "''")}'`;
  } else if (['integer', 'bigint', 'smallint', 'numeric'].includes(type)) {
    const numValue = Number(value);
    if (isNaN(numValue)) {
      console.warn(`Invalid numeric value: ${value}. Using NULL.`);
      return 'NULL';
    }
    return numValue.toString();
  } else {
    return `'${value}'`;
  }
}

@Injectable()
export class MigrationService {
  private readonly BATCH_SIZE = 500; 
  private readonly PROGRESS_FILE = path.join(process.cwd(), 'migration-progress.json');

  private async saveProgress(tableName: string, lastOffset: number): Promise<void> {
    const progress = {
      tableName,
      lastOffset,
      timestamp: new Date().toISOString()
    };
    await fs.promises.writeFile(this.PROGRESS_FILE, JSON.stringify(progress));
  }

  private async loadProgress(): Promise<{ tableName: string; lastOffset: number } | null> {
    try {
      if (fs.existsSync(this.PROGRESS_FILE)) {
        const data = await fs.promises.readFile(this.PROGRESS_FILE, 'utf8');
        return JSON.parse(data);
      }
    } catch (error) {
      console.warn('Failed to load progress:', error);
    }
    return null;
  }

  private async checkForeignKeyExists(mysqlConnection: DataSource, tableName: string): Promise<boolean> {
    const foreignKeys = await mysqlConnection.query(`
      SELECT COUNT(*) as count
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = ? 
        AND TABLE_NAME = ?
        AND REFERENCED_TABLE_NAME IS NOT NULL
    `, [mysqlConfig.database, tableName]);

    return foreignKeys[0].count > 0;
  }

  private async getMigrationOrder(mysqlConnection: DataSource): Promise<string[]> {
    const tables = await mysqlConnection.query(`
      SELECT TABLE_NAME 
      FROM INFORMATION_SCHEMA.TABLES 
      WHERE TABLE_SCHEMA = ?
    `, [mysqlConfig.database]);
    
    const tableNames = tables.map((table) => table.TABLE_NAME);

    // Jadvallar va ularning bog'liqliklarini saqlash uchun Map
    const dependencies = new Map<string, Set<string>>();
    const reverseDependencies = new Map<string, Set<string>>();

    // Barcha jadvallar uchun bo'sh Set yaratish
    tableNames.forEach(table => {
      dependencies.set(table, new Set());
      reverseDependencies.set(table, new Set());
    });

    // Bog'liqliklarni aniqlash
    for (const tableName of tableNames) {
      const foreignKeys = await mysqlConnection.query(`
        SELECT DISTINCT 
          TABLE_NAME,
          REFERENCED_TABLE_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = ? 
          AND TABLE_NAME = ?
          AND REFERENCED_TABLE_NAME IS NOT NULL
      `, [mysqlConfig.database, tableName]);

      for (const fk of foreignKeys) {
        const referencedTable = fk.REFERENCED_TABLE_NAME;
        dependencies.get(tableName)?.add(referencedTable);
        reverseDependencies.get(referencedTable)?.add(tableName);
      }
    }

    // Topological sort
    const order: string[] = [];
    const visited = new Set<string>();
    const visiting = new Set<string>();

    const circularDependencies = new Set<string>();
    
    const visit = (tableName: string, path: Set<string> = new Set()) => {
      if (visited.has(tableName)) return;
      if (path.has(tableName)) {
        circularDependencies.add(tableName);
        console.warn(`Circular dependency detected: ${Array.from(path).join(' -> ')} -> ${tableName}`);
        return;
      }

      visiting.add(tableName);
      path.add(tableName);

      for (const dep of dependencies.get(tableName) || []) {
        visit(dep, new Set(path));
      }

      path.delete(tableName);
      visiting.delete(tableName);
      visited.add(tableName);
      order.push(tableName);
    };

    // Avval hech qanday boshqa jadvalga bog'liq bo'lmagan jadvallardan boshlash
    const rootTables = tableNames.filter(table => 
      (dependencies.get(table)?.size || 0) === 0
    );

    // Agar root jadvallar bo'lmasa, istalgan jadvalni boshlang'ich nuqta sifatida olish
    const startTables = rootTables.length > 0 ? rootTables : tableNames;

    for (const table of startTables) {
      visit(table);
    }

    // Qolgan jadvallarni ham qo'shish
    tableNames.forEach(table => {
      if (!visited.has(table)) {
        visit(table);
      }
    });

    // Circular dependency bo'lgan jadvallarni oxirida ko'chirish
    const finalOrder = order.filter(table => !circularDependencies.has(table));
    circularDependencies.forEach(table => finalOrder.push(table));

    console.log('Migration order:', finalOrder);
    return finalOrder;
  }

  private async insertBatch(
    transactionalEntityManager: any,
    tableName: string,
    columns: any[],
    data: any[]
  ): Promise<void> {
    if (data.length === 0) return;

    const columnNames = columns.map(col => `"${col.COLUMN_NAME}"`).join(', ');
    const values = data.map(row => {
      const rowValues = columns.map(col => {
        const value = row[col.COLUMN_NAME];
        const type = col.DATA_TYPE.toLowerCase();
        const isNullable = col.IS_NULLABLE === 'YES';

        // Vaqt ma'lumotlarini to'g'ri formatga o'tkazish
        if (type === 'timestamp' || type === 'datetime' || type === 'date') {
          if (value && typeof value === 'string') {
            try {
              const cleanDate = value
                .replace(/GMT[+-]\d{4}.*$/, '')
                .trim();
              
              const date = new Date(cleanDate);
              if (!isNaN(date.valueOf())) {
                if (type === 'date') {
                  return `'${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}-${String(date.getUTCDate()).padStart(2, '0')}'`;
                } else {
                  return `'${date.toISOString().slice(0, 19).replace('T', ' ')}'`;
                }
              }
            } catch (err) {
              console.error(`Error parsing date: ${value}`, err);
            }
          }
          // Agar NOT NULL bo'lsa, joriy sanani qaytarish
          return isNullable ? 'NULL' : 'CURRENT_DATE';
        }

        if (value === null || value === undefined) {
          return 'NULL';
        }

        // Maxsus tiplar uchun qo'shimcha ishlov
        if (type === 'text' || type === 'varchar' || type === 'longtext' || type === 'mediumtext') {
          return `'${value.toString().replace(/'/g, "''")}'`;
        }

        if (['integer', 'bigint', 'smallint', 'numeric', 'double precision'].includes(type)) {
          const numValue = Number(value);
          return isNaN(numValue) ? 'NULL' : numValue;
        }

        return `'${value}'`;
      });
      return `(${rowValues.join(', ')})`;
    });

    const query = `INSERT INTO "${tableName}" (${columnNames}) VALUES ${values.join(', ')}`;
    await transactionalEntityManager.query(query);
  }

  async migrateData(): Promise<{ success: boolean; message: string }> {
    const mysqlConnection = new DataSource({
      ...(mysqlConfig as DataSourceOptions),
      name: 'mysql',
      logging: false,
      extra: {
        connectionLimit: 10,
        maxIdle: 10,
        idleTimeout: 60000,
      }
    });

    const postgresConnection = new DataSource({
      ...(postgresConfig as DataSourceOptions),
      name: 'postgres',
      logging: false,
      extra: {
        max: 20,
        maxUses: 7500,
        idleTimeoutMillis: 30000
      }
    });

    try {
      await mysqlConnection.initialize();
      await postgresConnection.initialize();

      const tableOrder = await this.getMigrationOrder(mysqlConnection);
      const progress = await this.loadProgress();

      let totalRecordsCount = 0;
      let currentProgress = 0;
      const tableRecordCounts = new Map<string, number>();

      // Barcha jadvallardagi ma'lumotlar sonini hisoblash
      for (const tableName of tableOrder) {
        const [{ count }] = await mysqlConnection.query(
          `SELECT COUNT(*) as count FROM \`${tableName}\``,
        );
        totalRecordsCount += count;
        tableRecordCounts.set(tableName, count);
      }

      console.log(`Total records to migrate: ${totalRecordsCount}`);

      const totalProgress = { 
        current: currentProgress, 
        total: totalRecordsCount 
      };

      const migratedTables = new Map<string, string>();
      const failedTables = new Set<string>();

      // Jadvallarni ko'chirish
      for (let i = 0; i < tableOrder.length; i++) {
        const tableName = tableOrder[i];
        try {
          await postgresConnection.transaction(async (transactionalEntityManager) => {
            const hasForeignKeys = await this.checkForeignKeyExists(mysqlConnection, tableName);
            if (hasForeignKeys) {
              await this.migrateTableWithForeignKeys(
                mysqlConnection, 
                transactionalEntityManager, 
                tableName, 
                progress, 
                totalProgress
              );
            } else {
              await this.migrateTableWithoutConstraints(
                mysqlConnection, 
                transactionalEntityManager, 
                tableName, 
                progress, 
                totalProgress
              );
            }
          });
          
          migratedTables.set(tableName, tableName.toLowerCase());
        } catch (error) {
          console.error(`\nError migrating table ${tableName}:`, error);
          failedTables.add(tableName);
        }
      }

      if (failedTables.size > 0) {
        const failedTablesList = Array.from(failedTables).join(', ');
        return {
          success: false,
          message: `Migration failed for tables: ${failedTablesList}`
        };
      }

      // Jadvallar mavjudligini tekshirish
      console.log('Verifying migrated tables...');
      for (const [originalName, lowerName] of migratedTables) {
        const [{ exists }] = await postgresConnection.query(`
          SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = $1
          ) as exists
        `, [lowerName]);

        if (!exists) {
          throw new Error(`Table ${originalName} was not properly migrated (looking for ${lowerName})`);
        }
      }

      // Failed foreign key'larni saqlash uchun
      const failedForeignKeys: Array<{
        tableName: string;
        columnName: string;
        referencedTable: string;
        referencedColumn: string;
      }> = [];

      // So'ngra foreign key'larni qo'shish
      console.log('Adding foreign key constraints...');
      for (const tableName of tableOrder) {
        if (!migratedTables.has(tableName)) {
          console.warn(`Skipping foreign keys for non-migrated table: ${tableName}`);
          continue;
        }

        await postgresConnection.transaction(async (transactionalEntityManager) => {
          try {
            const failed = await this.addForeignKeyConstraints(mysqlConnection, transactionalEntityManager, tableName);
            failedForeignKeys.push(...failed);
            console.log(`Successfully added foreign keys for table: ${tableName}`);
          } catch (error) {
            console.error(`Failed to add foreign keys for table ${tableName}:`, error);
            console.warn('Continuing despite foreign key error');
          }
        });
      }

      // Muvaffaqiyatsiz foreign key'larni qayta qo'shishga urinish
      if (failedForeignKeys.length > 0) {
        console.log('Retrying failed foreign key constraints...');
        for (const fk of failedForeignKeys) {
          await postgresConnection.transaction(async (transactionalEntityManager) => {
            try {
              await transactionalEntityManager.query(`
                ALTER TABLE "${fk.tableName}" 
                ADD CONSTRAINT "FK_${fk.tableName}_${fk.columnName}" 
                FOREIGN KEY ("${fk.columnName}") 
                REFERENCES "${fk.referencedTable}" ("${fk.referencedColumn}")
                ON DELETE CASCADE
                ON UPDATE CASCADE
              `);
              console.log(`Successfully added foreign key for ${fk.tableName}.${fk.columnName}`);
            } catch (err) {
              console.error(`Failed to add foreign key on retry for ${fk.tableName}.${fk.columnName}:`, err);
            }
          });
        }
      }

      if (fs.existsSync(this.PROGRESS_FILE)) {
        await fs.promises.unlink(this.PROGRESS_FILE);
      }

      return {
        success: true,
        message: 'Data migration completed successfully',
      };
    } catch (error) {
      console.error('Migration failed:', error);
      return {
        success: false,
        message: `Migration failed: ${error.message}`,
      };
    } finally {
      if (mysqlConnection?.isInitialized) await mysqlConnection.destroy();
      if (postgresConnection?.isInitialized) await postgresConnection.destroy();
    }
  }

  private async migrateTableWithForeignKeys(
    mysqlConnection: DataSource,
    transactionalEntityManager: any,
    tableName: string,
    progress: any,
    totalProgress: { current: number, total: number }
  ): Promise<void> {
    try {
      console.log(`Starting migration for table: ${tableName}`);
      
      const columns = await mysqlConnection.query(`
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA, COLUMN_DEFAULT, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
      `, [mysqlConfig.database, tableName]);

      if (!columns || columns.length === 0) {
        throw new Error(`No columns found for table ${tableName}`);
      }

      console.log(`Found ${columns.length} columns for table ${tableName}`);

      let createTableSQL = `CREATE TABLE "${tableName}" (`;
      const columnDefinitions = columns.map((column) => {
        let type = column['DATA_TYPE'].toLowerCase();

        // Ma'lumot turlarini konvertatsiya
        switch (type) {
          case 'int':
            type = column.EXTRA === 'auto_increment' ? 'SERIAL' : 'INTEGER';
            break;
          case 'bigint':
            type = column.EXTRA === 'auto_increment' ? 'BIGSERIAL' : 'BIGINT';
            break;
          case 'enum':
            type = 'TEXT'; // ENUM o'rniga TEXT ishlatamiz
            break;
          case 'datetime':
          case 'timestamp':
            type = 'TIMESTAMP';
            break;
          case 'double':
            type = 'DOUBLE PRECISION'; // DOUBLE o'rniga DOUBLE PRECISION
            break;
          case 'longtext':
          case 'varchar':
            type = 'TEXT';
            break;
          case 'tinyint':
            type = 'SMALLINT';
            break;
          case 'decimal':
          case 'numeric':
            type = `NUMERIC(${column.NUMERIC_PRECISION || 10}, ${column.NUMERIC_SCALE || 2})`;
            break;
        }

        let definition = `"${column.COLUMN_NAME}" ${type}`;

        if (column.IS_NULLABLE === 'NO') {
          if (type !== 'SERIAL' && type !== 'BIGSERIAL') {
            definition += ' NOT NULL';
            if (column.COLUMN_DEFAULT === null) {
              switch (type) {
                case 'INTEGER':
                case 'BIGINT':
                case 'SMALLINT':
                  definition += ' DEFAULT 0';
                  break;
                case 'TEXT':
                  definition += ` DEFAULT ''`;
                  break;
                case 'TIMESTAMP':
                  definition += ` DEFAULT NULL`;
                  break;
                case 'DOUBLE PRECISION':
                  definition += ' DEFAULT 0';
                  break;
                case 'NUMERIC':
                  definition += ' DEFAULT 0';
                  break;
              }
            } else {
              const defaultValue = column.COLUMN_DEFAULT.toString();
              if (type === 'TEXT') {
                definition += ` DEFAULT '${defaultValue.replace(/'/g, "''")}'`;
              } else if (type === 'TIMESTAMP') {
                definition += ` DEFAULT NULL`;
              } else {
                definition += ` DEFAULT ${defaultValue}`;
              }
            }
          }
        }

        if (column.COLUMN_KEY === 'PRI' && !['SERIAL', 'BIGSERIAL'].includes(type)) {
          definition += ' PRIMARY KEY';
        }

        return definition;
      });

      createTableSQL += columnDefinitions.join(', ') + ')';

      console.log(`Dropping existing table if exists: ${tableName}`);
      await transactionalEntityManager.query(`DROP TABLE IF EXISTS "${tableName}" CASCADE`);
      
      console.log(`Creating table: ${tableName}`);
      await transactionalEntityManager.query(createTableSQL);
      
      console.log(`Table ${tableName} created successfully`);

      // Ma'lumotlarni ko'chirish
      await this.migrateTableData(mysqlConnection, transactionalEntityManager, tableName, columns, progress, totalProgress);

      // Jadval yaratilganini tekshirish
      const [{ exists }] = await transactionalEntityManager.query(`
        SELECT EXISTS (
          SELECT 1 
          FROM information_schema.tables 
          WHERE table_schema = 'public' 
          AND table_name = $1
        ) as exists
      `, [tableName.toLowerCase()]);

      if (!exists) {
        throw new Error(`Failed to create table ${tableName}`);
      }

      console.log(`Successfully completed migration for table: ${tableName}`);
    } catch (error) {
      console.error(`Error migrating table ${tableName}:`, error);
      throw error;
    }
  }

  private async migrateTableWithoutConstraints(
    mysqlConnection: DataSource,
    transactionalEntityManager: any,
    tableName: string,
    progress: any,
    totalProgress: { current: number, total: number }
  ): Promise<void> {
    try {
      const columns = await mysqlConnection.query(`
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA, COLUMN_DEFAULT, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
      `, [mysqlConfig.database, tableName]);

      let createTableSQL = `CREATE TABLE "${tableName}" (`;
      const columnDefinitions = columns.map((column) => {
        let type = column['DATA_TYPE'].toLowerCase();
        let definition = `"${column.COLUMN_NAME}" `;

        // Ma'lumot turlarini konvertatsiya
        switch (type) {
          case 'int':
            type = column.EXTRA === 'auto_increment' ? 'SERIAL' : 'INTEGER';
            break;
          case 'bigint':
            type = column.EXTRA === 'auto_increment' ? 'BIGSERIAL' : 'BIGINT';
            break;
          case 'enum':
            type = 'TEXT';
            break;
          case 'datetime':
          case 'timestamp':
            type = 'TIMESTAMP';
            break;
          case 'date':
            type = 'DATE';
            break;
          case 'double':
            type = 'DOUBLE PRECISION';
            break;
          case 'mediumtext':
          case 'longtext':
          case 'varchar':
            type = 'TEXT';
            break;
          case 'tinyint':
            type = 'SMALLINT';
            break;
          case 'decimal':
          case 'numeric':
            type = `NUMERIC(${column.NUMERIC_PRECISION || 10}, ${column.NUMERIC_SCALE || 2})`;
            break;
        }

        definition += type;

        // Faqat SERIAL va BIGSERIAL uchun NOT NULL cheklovini qo'yamiz
        if (type === 'SERIAL' || type === 'BIGSERIAL') {
          definition += ' NOT NULL';
        }

        // Column default qiymatini qo'shish
        if (column.COLUMN_DEFAULT !== null) {
          const defaultValue = column.COLUMN_DEFAULT.toString();
          if (type === 'TEXT') {
            definition += ` DEFAULT '${defaultValue.replace(/'/g, "''")}'`;
          } else if (type === 'TIMESTAMP' || type === 'DATE') {
            // TIMESTAMP default qiymatini to'g'ri formatda berish
            if (defaultValue.toLowerCase() === 'current_timestamp') {
              definition += ` DEFAULT CURRENT_TIMESTAMP`;
            } else {
              try {
                const date = new Date(defaultValue);
                if (!isNaN(date.valueOf())) {
                  definition += ` DEFAULT '${date.toISOString().slice(0, 19).replace('T', ' ')}'`;
                }
              } catch (err) {
                console.warn(`Invalid date default value: ${defaultValue}. Skipping default.`);
              }
            }
          } else {
            definition += ` DEFAULT ${defaultValue}`;
          }
        }

        return definition;
      });

      createTableSQL += columnDefinitions.join(', ') + ')';

      await transactionalEntityManager.query(`DROP TABLE IF EXISTS "${tableName}" CASCADE`);
      await transactionalEntityManager.query(createTableSQL);

      await this.migrateTableData(mysqlConnection, transactionalEntityManager, tableName, columns, progress, totalProgress);

      // 3. NOT NULL cheklovlarini qayta o'rnatish
      await this.restoreNotNullConstraints(mysqlConnection, transactionalEntityManager, tableName);
    } catch (error) {
      console.error(`Error migrating table ${tableName}:`, error);
      throw error;
    }
  }

  private async migrateTableData(
    mysqlConnection: DataSource,
    transactionalEntityManager: any,
    tableName: string,
    columns: any[],
    progress: any,
    totalProgress: { current: number, total: number }
  ): Promise<void> {
    const [{ count }] = await mysqlConnection.query(
      `SELECT COUNT(*) as count FROM \`${tableName}\``,
    );

    let startBatch = 0;
    if (progress && progress.tableName === tableName) {
      startBatch = Math.floor(progress.lastOffset / this.BATCH_SIZE);
    }

    let totalMigrated = startBatch * this.BATCH_SIZE;
    const totalBatches = Math.ceil(count / this.BATCH_SIZE);

    for (let batch = startBatch; batch < totalBatches; batch++) {
      const offset = batch * this.BATCH_SIZE;
      
      if (batch % 10 === 0) {
        global.gc?.();
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      const data = await mysqlConnection.query(
        `SELECT * FROM \`${tableName}\` LIMIT ? OFFSET ?`,
        [this.BATCH_SIZE, offset],
      );

      if (data.length > 0) {
        try {
          await this.insertBatch(transactionalEntityManager, tableName, columns, data);
          totalMigrated += data.length;
          totalProgress.current += data.length;
          
          // Progress ko'rsatish
          process.stdout.write(
            `\rProcessing: ${tableName} | ` +
            `Table Progress: ${Math.round((totalMigrated/count)*100)}% | ` +
            `Total Progress: ${Math.round((totalProgress.current/totalProgress.total)*100)}%`
          );
          
          await this.saveProgress(tableName, offset + data.length);
        } catch (error) {
          console.error(`\nError inserting data for table ${tableName}:`, error);
          throw error;
        }
      }
    }
    console.log(); // Yangi qatorga o'tish
  }

  private async addForeignKeyConstraints(
    mysqlConnection: DataSource,
    transactionalEntityManager: any,
    tableName: string
  ): Promise<Array<{
    tableName: string;
    columnName: string;
    referencedTable: string;
    referencedColumn: string;
  }>> {
    const failedForeignKeys: Array<{
      tableName: string;
      columnName: string;
      referencedTable: string;
      referencedColumn: string;
    }> = [];

    // Avval jadval mavjudligini tekshirish
    const [{ exists }] = await transactionalEntityManager.query(`
      SELECT EXISTS (
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name = $1
      ) as exists
    `, [tableName.toLowerCase()]);

    if (!exists) {
      throw new Error(`Table ${tableName} does not exist in PostgreSQL`);
    }

    const foreignKeys = await mysqlConnection.query(`
      SELECT 
        COLUMN_NAME,
        REFERENCED_TABLE_NAME,
        REFERENCED_COLUMN_NAME
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = ?
        AND TABLE_NAME = ? 
        AND REFERENCED_TABLE_NAME IS NOT NULL
    `, [mysqlConfig.database, tableName]);

    for (const fk of foreignKeys) {
      try {
        // Referenced jadval mavjudligini tekshirish
        const [{ exists: referencedExists }] = await transactionalEntityManager.query(`
          SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            AND table_name = $1
          ) as exists
        `, [fk.REFERENCED_TABLE_NAME.toLowerCase()]);

        if (!referencedExists) {
          console.warn(`Referenced table ${fk.REFERENCED_TABLE_NAME} does not exist. Skipping foreign key.`);
          continue;
        }

        await transactionalEntityManager.query(`
          ALTER TABLE "${tableName}" 
          ADD CONSTRAINT "FK_${tableName}_${fk.COLUMN_NAME}" 
          FOREIGN KEY ("${fk.COLUMN_NAME}") 
          REFERENCES "${fk.REFERENCED_TABLE_NAME}" ("${fk.REFERENCED_COLUMN_NAME}")
          ON DELETE CASCADE
          ON UPDATE CASCADE
        `);
        console.log(`Added foreign key for ${tableName}.${fk.COLUMN_NAME}`);
      } catch (err) {
        console.error(`Failed to add foreign key for ${tableName}.${fk.COLUMN_NAME}:`, err);
        failedForeignKeys.push({
          tableName,
          columnName: fk.COLUMN_NAME,
          referencedTable: fk.REFERENCED_TABLE_NAME,
          referencedColumn: fk.REFERENCED_COLUMN_NAME
        });
        console.warn('Continuing despite foreign key error');
      }
    }

    return failedForeignKeys;
  }

  private async restoreNotNullConstraints(
    mysqlConnection: DataSource,
    transactionalEntityManager: any,
    tableName: string
  ): Promise<void> {
    try {
      // MySQL'dan NOT NULL ustunlarni olish
      const columns = await mysqlConnection.query(`
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = ? 
        AND TABLE_NAME = ?
        AND IS_NULLABLE = 'NO'
      `, [mysqlConfig.database, tableName]);

      // NOT NULL cheklovlarini qayta qo'shish
      for (const column of columns) {
        const columnName = column.COLUMN_NAME;
        const type = column.DATA_TYPE.toLowerCase();
        
        // SERIAL va BIGSERIAL ustunlarini o'tkazib yuborish chunki ular allaqachon NOT NULL
        if (!['SERIAL', 'BIGSERIAL'].includes(type)) {
          try {
            await transactionalEntityManager.query(`
              ALTER TABLE "${tableName}" 
              ALTER COLUMN "${columnName}" SET NOT NULL
            `);
          } catch (error) {
            console.error(`Error setting NOT NULL constraint for ${tableName}.${columnName}:`, error);
            throw error;
          }
        }
      }
    } catch (error) {
      console.error(`Error restoring NOT NULL constraints for table ${tableName}:`, error);
      throw error;
    }
  }
}