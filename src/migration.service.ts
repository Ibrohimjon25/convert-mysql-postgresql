import { Injectable } from '@nestjs/common';
import { DataSource, DataSourceOptions } from 'typeorm';
import { mysqlConfig, postgresConfig } from './database.config';
import * as fs from 'fs';
import * as path from 'path';

@Injectable()
export class MigrationService {
  private readonly BATCH_SIZE = 500;
  private readonly PROGRESS_FILE = path.join(process.cwd(), 'migration-progress.json');

  private async saveProgress(tableName: string, lastOffset: number): Promise<void> {
    const progress = { tableName, lastOffset, timestamp: new Date().toISOString() };
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

  private async getMigrationOrder(mysqlConnection: DataSource): Promise<string[]> {
    const tables = await mysqlConnection.query(`
      SELECT TABLE_NAME 
      FROM INFORMATION_SCHEMA.TABLES 
      WHERE TABLE_SCHEMA = ?
    `, [mysqlConfig.database]);

    const tableNames = tables.map((table) => table.TABLE_NAME);
    const dependencies = new Map<string, Set<string>>();
    const reverseDependencies = new Map<string, Set<string>>();

    tableNames.forEach((table) => {
      dependencies.set(table, new Set());
      reverseDependencies.set(table, new Set());
    });

    for (const tableName of tableNames) {
      const foreignKeys = await mysqlConnection.query(`
        SELECT DISTINCT REFERENCED_TABLE_NAME
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

    const order: string[] = [];
    const visited = new Set<string>();
    const visiting = new Set<string>();

    const visit = (tableName: string) => {
      if (visited.has(tableName)) return;
      if (visiting.has(tableName)) {
        console.warn(`Circular dependency detected involving ${tableName}`);
        return;
      }

      visiting.add(tableName);
      for (const dep of dependencies.get(tableName) || []) {
        visit(dep);
      }
      visiting.delete(tableName);
      visited.add(tableName);
      order.push(tableName);
    };

    tableNames.forEach((table) => visit(table));
    console.log('Migration order:', order);
    return order;
  }

  private async insertBatch(
    transactionalEntityManager: any,
    tableName: string,
    columns: any[],
    data: any[]
  ): Promise<void> {
    if (data.length === 0) return;

    const columnNames = columns.map((col) => `"${col.COLUMN_NAME}"`).join(', ');
    const values = data.map((row) => {
      const rowValues = columns.map((col) => {
        const value = row[col.COLUMN_NAME];
        const type = col.DATA_TYPE.toLowerCase();
        const isNullable = col.IS_NULLABLE === 'YES';

        if (value === null || value === undefined) return isNullable ? 'NULL' : '0';
        if (type.includes('int') || type === 'numeric' || type === 'double') return value;
        if (type === 'timestamp' || type === 'datetime') {
          return `'${new Date(value).toISOString()}'`; // UTC vaqtni ISO formatda saqlaymiz
        }
        return `'${value.toString().replace(/'/g, "''")}'`;
      });
      return `(${rowValues.join(', ')})`;
    });

    const query = `INSERT INTO "public"."${tableName.toLowerCase()}" (${columnNames}) VALUES ${values.join(', ')}`;
    await transactionalEntityManager.query(query);
  }

  async migrateData(): Promise<{ success: boolean; message: string }> {
    const mysqlConnection = new DataSource({ ...(mysqlConfig as DataSourceOptions), name: 'mysql', logging: false });
    const postgresConnection = new DataSource({ ...(postgresConfig as DataSourceOptions), name: 'postgres', logging: false });

    try {
      await mysqlConnection.initialize();
      await postgresConnection.initialize();

      const tableOrder = await this.getMigrationOrder(mysqlConnection);
      const progress = await this.loadProgress();

      let totalRecordsCount = 0;
      for (const tableName of tableOrder) {
        const [{ count }] = await mysqlConnection.query(`SELECT COUNT(*) as count FROM \`${tableName}\``);
        totalRecordsCount += parseInt(count);
      }

      const totalProgress = { current: 0, total: totalRecordsCount };
      const migratedTables = new Set<string>();

      for (const tableName of tableOrder) {
        await postgresConnection.transaction(async (transactionalEntityManager) => {
          await this.migrateTable(mysqlConnection, transactionalEntityManager, tableName, progress, totalProgress);
          migratedTables.add(tableName.toLowerCase());
        });
      }

      console.log('Adding foreign key constraints...');
      for (const tableName of tableOrder) {
        if (!migratedTables.has(tableName.toLowerCase())) continue;
        const failedFKs = await this.addForeignKeyConstraints(mysqlConnection, postgresConnection, tableName);
        if (failedFKs.length > 0) {
          console.warn(`Retrying failed foreign keys for ${tableName}`);
          for (const fk of failedFKs) {
            await postgresConnection.transaction(async (manager) => {
              await this.ensureUniqueConstraint(manager, fk.referencedTable, fk.referencedColumn);
              await manager.query(`
                ALTER TABLE "public"."${fk.tableName}"
                ADD CONSTRAINT "fk_${fk.tableName}_${fk.columnName}"
                FOREIGN KEY ("${fk.columnName}")
                REFERENCES "public"."${fk.referencedTable}" ("${fk.referencedColumn}")
                ON DELETE CASCADE
                ON UPDATE CASCADE
              `);
            });
          }
        }
      }

      if (fs.existsSync(this.PROGRESS_FILE)) await fs.promises.unlink(this.PROGRESS_FILE);

      return { success: true, message: 'Data migration completed successfully' };
    } catch (error) {
      console.error('Migration failed:', error);
      return { success: false, message: `Migration failed: ${error.message}` };
    } finally {
      if (mysqlConnection.isInitialized) await mysqlConnection.destroy();
      if (postgresConnection.isInitialized) await postgresConnection.destroy();
    }
  }

  private async migrateTable(
    mysqlConnection: DataSource,
    transactionalEntityManager: any,
    tableName: string,
    progress: any,
    totalProgress: { current: number; total: number }
  ): Promise<void> {
    const columns = await mysqlConnection.query(`
      SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA
      FROM INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    `, [mysqlConfig.database, tableName]);

    let createTableSQL = `CREATE TABLE "public"."${tableName.toLowerCase()}" (`;
    const columnDefinitions = columns.map((column) => {
      let type = column.DATA_TYPE.toLowerCase();
      switch (type) {
        case 'int':
          type = column.EXTRA === 'auto_increment' ? 'SERIAL' : 'INTEGER';
          break;
        case 'bigint':
          type = column.EXTRA === 'auto_increment' ? 'BIGSERIAL' : 'BIGINT';
          break;
        case 'varchar':
        case 'longtext':
          type = 'TEXT';
          break;
        case 'datetime':
        case 'timestamp':
          type = 'TIMESTAMPTZ'; // Vaqt mintaqasi bilan saqlash uchun
          break;
        case 'double':
          type = 'DOUBLE PRECISION';
          break;
        default:
          type = 'TEXT';
      }

      let definition = `"${column.COLUMN_NAME}" ${type}`;
      if (column.COLUMN_KEY === 'PRI') definition += ' PRIMARY KEY';
      if (column.COLUMN_KEY === 'UNI') definition += ' UNIQUE';
      if (column.IS_NULLABLE === 'NO' && !type.includes('SERIAL')) definition += ' NOT NULL';

      return definition;
    });

    createTableSQL += columnDefinitions.join(', ') + ')';
    await transactionalEntityManager.query(`DROP TABLE IF EXISTS "public"."${tableName.toLowerCase()}" CASCADE`);
    await transactionalEntityManager.query(createTableSQL);

    const [{ count }] = await mysqlConnection.query(`SELECT COUNT(*) as count FROM \`${tableName}\``);
    let startBatch = progress && progress.tableName === tableName ? Math.floor(progress.lastOffset / this.BATCH_SIZE) : 0;
    const totalBatches = Math.ceil(count / this.BATCH_SIZE);

    for (let batch = startBatch; batch < totalBatches; batch++) {
      const offset = batch * this.BATCH_SIZE;
      const selectQuery = `
        SELECT ${columns.map(col => `\`${col.COLUMN_NAME}\``).join(', ')} 
        FROM \`${tableName}\` 
        LIMIT ? OFFSET ?
      `;
      const data = await mysqlConnection.query(selectQuery, [this.BATCH_SIZE, offset]);

      if (data.length > 0) {
        await this.insertBatch(transactionalEntityManager, tableName, columns, data);
        totalProgress.current += data.length;
        await this.saveProgress(tableName, offset + data.length);
        console.log(`Progress for ${tableName}: ${Math.round(((batch + 1) / totalBatches) * 100)}%`);
      }
    }
  }

  private async ensureUniqueConstraint(manager: any, tableName: string, columnName: string): Promise<void> {
    const hasUnique = await manager.query(`
      SELECT EXISTS (
        SELECT 1 
        FROM information_schema.table_constraints tc
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
        WHERE tc.table_schema = 'public'
          AND tc.table_name = $1
          AND ccu.column_name = $2
          AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
      ) as exists
    `, [tableName.toLowerCase(), columnName.toLowerCase()]);

    if (!hasUnique[0].exists) {
      await manager.query(`
        ALTER TABLE "public"."${tableName.toLowerCase()}"
        ADD CONSTRAINT "unique_${tableName.toLowerCase()}_${columnName.toLowerCase()}"
        UNIQUE ("${columnName.toLowerCase()}")
      `);
      console.log(`Added UNIQUE constraint to ${tableName}.${columnName}`);
    }
  }

  private async addForeignKeyConstraints(
    mysqlConnection: DataSource,
    postgresConnection: DataSource,
    tableName: string
  ): Promise<Array<{ tableName: string; columnName: string; referencedTable: string; referencedColumn: string }>> {
    const foreignKeys = await mysqlConnection.query(`
      SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL
    `, [mysqlConfig.database, tableName]);

    const failedFKs: Array<{ tableName: string; columnName: string; referencedTable: string; referencedColumn: string }> = [];

    for (const fk of foreignKeys) {
      try {
        await postgresConnection.transaction(async (manager) => {
          await this.ensureUniqueConstraint(manager, fk.REFERENCED_TABLE_NAME, fk.REFERENCED_COLUMN_NAME);
          await manager.query(`
            ALTER TABLE "public"."${tableName.toLowerCase()}"
            ADD CONSTRAINT "fk_${tableName.toLowerCase()}_${fk.COLUMN_NAME.toLowerCase()}"
            FOREIGN KEY ("${fk.COLUMN_NAME.toLowerCase()}")
            REFERENCES "public"."${fk.REFERENCED_TABLE_NAME.toLowerCase()}" ("${fk.REFERENCED_COLUMN_NAME.toLowerCase()}")
            ON DELETE CASCADE
            ON UPDATE CASCADE
          `);
          console.log(`Added FK for ${tableName}.${fk.COLUMN_NAME}`);
        });
      } catch (err) {
        console.error(`Failed to add FK for ${tableName}.${fk.COLUMN_NAME}:`, err);
        failedFKs.push({
          tableName: tableName.toLowerCase(),
          columnName: fk.COLUMN_NAME.toLowerCase(),
          referencedTable: fk.REFERENCED_TABLE_NAME.toLowerCase(),
          referencedColumn: fk.REFERENCED_COLUMN_NAME.toLowerCase(),
        });
      }
    }

    return failedFKs;
  }
}