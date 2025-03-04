import { Injectable } from '@nestjs/common';
import { DataSource, DataSourceOptions } from 'typeorm';
import { mysqlConfig, postgresConfig } from './database.config';
import * as fs from 'fs';
import * as path from 'path';

@Injectable()
export class MigrationService {
  private readonly BATCH_SIZE = 10000;
  private readonly PARALLEL_LIMIT = 8;
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
      console.error('Failed to load progress:', error);
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

    tableNames.forEach((table) => dependencies.set(table, new Set()));

    for (const tableName of tableNames) {
      const foreignKeys = await mysqlConnection.query(`
        SELECT DISTINCT REFERENCED_TABLE_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = ? 
          AND TABLE_NAME = ?
          AND REFERENCED_TABLE_NAME IS NOT NULL
      `, [mysqlConfig.database, tableName]);

      for (const fk of foreignKeys) {
        dependencies.get(tableName)?.add(fk.REFERENCED_TABLE_NAME);
      }
    }

    const order: string[] = [];
    const visited = new Set<string>();
    const visiting = new Set<string>();

    const visit = (tableName: string) => {
      if (visited.has(tableName)) return;
      if (visiting.has(tableName)) return;

      visiting.add(tableName);
      for (const dep of dependencies.get(tableName) || []) {
        visit(dep);
      }
      visiting.delete(tableName);
      visited.add(tableName);
      order.push(tableName);
    };

    tableNames.forEach((table) => visit(table));
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
          return `'${new Date(value).toISOString()}'`;
        }
        return `'${value.toString().replace(/'/g, "''")}'`;
      });
      return `(${rowValues.join(', ')})`;
    });

    const query = `INSERT INTO "public"."${tableName.toLowerCase()}" (${columnNames}) VALUES ${values.join(', ')} RETURNING *`;
    let result;
    try {
      result = await transactionalEntityManager.query(query);
    } catch (error) {
      console.error(`PostgreSQL insert failed for ${tableName}:`, error.message);
      throw error; // Xatolikni yuqoriga ko‘tarish
    }

    const insertedRows = result ? result.length : 0;
    if (insertedRows !== data.length) {
      console.error(`Insert mismatch for ${tableName}: expected ${data.length}, inserted ${insertedRows}`);
      throw new Error(`Inserted rows (${insertedRows}) do not match expected (${data.length}) for table ${tableName}`);
    }
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

      console.log(`Total records to migrate: ${totalRecordsCount}`);
      console.time('Migration');

      const migrateTables = async (tables: string[]) => {
        await Promise.all(
          tables.map((tableName) =>
            postgresConnection.transaction(async (transactionalEntityManager) => {
              await this.migrateTable(mysqlConnection, transactionalEntityManager, tableName, progress, totalProgress);
              const progressPercentage = ((totalProgress.current / totalProgress.total) * 100).toFixed(2);
              console.log(`Progress: ${progressPercentage}% (${totalProgress.current}/${totalProgress.total} records)`);
            })
          )
        );
      };

      for (let i = 0; i < tableOrder.length; i += this.PARALLEL_LIMIT) {
        const chunk = tableOrder.slice(i, i + this.PARALLEL_LIMIT);
        await migrateTables(chunk);
      }

      console.log('Verifying data integrity...');
      await this.verifyDataIntegrity(mysqlConnection, postgresConnection, tableOrder);

      console.log('Checking foreign key compatibility...');
      const fkIssues = await this.checkForeignKeyCompatibility(postgresConnection);
      if (fkIssues.length > 0) {
        throw new Error(`Foreign key issues detected: ${JSON.stringify(fkIssues)}`);
      }

      console.log('Adding foreign key constraints...');
      for (const tableName of tableOrder) {
        await this.addForeignKeyConstraints(mysqlConnection, postgresConnection, tableName);
      }

      console.timeEnd('Migration');
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
          type = 'TIMESTAMPTZ';
          break;
        case 'double':
          type = 'DOUBLE PRECISION';
          break;
        default:
          type = 'TEXT';
      }

      let definition = `"${column.COLUMN_NAME}" ${type}`;
      if (column.COLUMN_KEY === 'PRI') definition += ' PRIMARY KEY';
      if (column.IS_NULLABLE === 'NO' && !type.includes('SERIAL')) definition += ' NOT NULL';
      return definition;
    });

    createTableSQL += columnDefinitions.join(', ') + ')';
    await transactionalEntityManager.query(`DROP TABLE IF EXISTS "public"."${tableName.toLowerCase()}" CASCADE`);
    await transactionalEntityManager.query(createTableSQL);

    const [{ count: mysqlCount }] = await mysqlConnection.query(`SELECT COUNT(*) as count FROM \`${tableName}\``);
    let startOffset = progress && progress.tableName === tableName ? progress.lastOffset : 0;
    let processedRows = 0;

    while (processedRows < mysqlCount) {
      const selectQuery = `SELECT * FROM \`${tableName}\` LIMIT ? OFFSET ?`;
      const data = await mysqlConnection.query(selectQuery, [this.BATCH_SIZE, startOffset]);

      if (data.length === 0) {
        if (processedRows < mysqlCount) {
          throw new Error(`No data returned for ${tableName} at offset ${startOffset}, expected ${mysqlCount - processedRows} more rows`);
        }
        break;
      }

      try {
        await this.insertBatch(transactionalEntityManager, tableName, columns, data);
        processedRows += data.length;
        startOffset += data.length;
        totalProgress.current += data.length;
        await this.saveProgress(tableName, startOffset);
      } catch (error) {
        console.error(`Failed to process batch for ${tableName} at offset ${startOffset}:`, error.message);
        throw error;
      }
    }

    const [{ count: postgresCount }] = await transactionalEntityManager.query(`SELECT COUNT(*) as count FROM "public"."${tableName.toLowerCase()}"`);
    if (mysqlCount !== postgresCount) {
      throw new Error(`Row count mismatch for ${tableName}: MySQL=${mysqlCount}, PostgreSQL=${postgresCount}`);
    }
  }

  private async verifyDataIntegrity(mysqlConnection: DataSource, postgresConnection: DataSource, tableOrder: string[]): Promise<void> {
    for (const tableName of tableOrder) {
      const [{ count: mysqlCount }] = await mysqlConnection.query(`SELECT COUNT(*) as count FROM \`${tableName}\``);
      const [{ count: postgresCount }] = await postgresConnection.query(`SELECT COUNT(*) as count FROM "public"."${tableName.toLowerCase()}"`);
      if (mysqlCount !== postgresCount) {
        throw new Error(`Data integrity check failed for ${tableName}: MySQL=${mysqlCount}, PostgreSQL=${postgresCount}`);
      }
    }
    console.log('Data integrity verified successfully');
  }

  private async checkForeignKeyCompatibility(postgresConnection: DataSource): Promise<Array<{ table: string; column: string; refTable: string; refColumn: string; orphanedCount: number }>> {
    const fkChecks = [
      { table: 'canceled_pre_order_product_infos', column: 'product_variant_id', refTable: 'product_variants', refColumn: 'id' },
      { table: 'special_category_products', column: 'product_variant_id', refTable: 'product_variants', refColumn: 'id' },
    ];

    const issues: Array<{ table: string; column: string; refTable: string; refColumn: string; orphanedCount: number }> = [];
    for (const { table, column, refTable, refColumn } of fkChecks) {
      const query = `
        SELECT COUNT(*) as orphaned
        FROM "public"."${table}"
        WHERE "${column}" IS NOT NULL
        AND "${column}" NOT IN (SELECT "${refColumn}" FROM "public"."${refTable}")
      `;
      const [{ orphaned }] = await postgresConnection.query(query);
      if (orphaned > 0) {
        issues.push({ table, column, refTable, refColumn, orphanedCount: Number(orphaned) });
        console.log(`Found ${orphaned} orphaned rows in ${table}.${column}`);
      }
    }
    return issues;
  }

  private async addForeignKeyConstraints(
    mysqlConnection: DataSource,
    postgresConnection: DataSource,
    tableName: string
  ): Promise<void> {
    const foreignKeys = await mysqlConnection.query(`
      SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL
    `, [mysqlConfig.database, tableName]);

    for (const fk of foreignKeys) {
      try {
        await postgresConnection.query(`
          ALTER TABLE "public"."${tableName.toLowerCase()}"
          ADD CONSTRAINT "fk_${tableName.toLowerCase()}_${fk.COLUMN_NAME.toLowerCase()}"
          FOREIGN KEY ("${fk.COLUMN_NAME.toLowerCase()}")
          REFERENCES "public"."${fk.REFERENCED_TABLE_NAME.toLowerCase()}" ("${fk.REFERENCED_COLUMN_NAME.toLowerCase()}")
          ON DELETE CASCADE
          ON UPDATE CASCADE
        `);
      } catch (err) {
        throw new Error(`Failed to add FK for ${tableName}.${fk.COLUMN_NAME}: ${err.message}`);
      }
    }
  }
}