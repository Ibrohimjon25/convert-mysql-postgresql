import { Injectable } from '@nestjs/common';
import { DataSource, DataSourceOptions } from 'typeorm';
import { mysqlConfig, postgresConfig } from './database.config';
import * as fs from 'fs';
import * as path from 'path';

function formatValue(value: any, type: string): string {
  if (type === 'timestamp' || type === 'datetime') {
    if (value === null || value === undefined) {
      return 'CURRENT_TIMESTAMP';
    }

    let cleanValue = value;
    if (typeof value === 'string') {
      cleanValue = value.replace(/GMT\+[0-9]{4}/i, '').trim();
    }

    try {
      const date = new Date(cleanValue);
      if (!isNaN(date.valueOf())) {
        return `'${date.toISOString().replace('T', ' ').replace('Z', '')}'`;
      } else {
        console.warn(`Invalid date value: ${value}. Using CURRENT_TIMESTAMP.`);
        return 'CURRENT_TIMESTAMP';
      }
    } catch (err) {
      console.error(`Error parsing date: ${value}.`, err);
      return 'CURRENT_TIMESTAMP';
    }
  } else if (type === 'text' || type === 'varchar' || type === 'longtext') {
    return `'${value.toString().replace(/'/g, "''")}'`;
  } else if (['integer', 'bigint', 'smallint', 'numeric'].includes(type)) {
    return value === null || value === undefined ? 'NULL' : value.toString();
  } else {
    return `'${value}'`;
  }
}

@Injectable()
export class MigrationService {
  private readonly BATCH_SIZE = 500; // Kamaytirilgan batch hajmi
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
    const tables = await mysqlConnection.query('SHOW TABLES');
    const tableNames = tables.map((table) => Object.values(table)[0]);

    const graph = new Map<string, Set<string>>();
    for (const tableName of tableNames) {
      const foreignKeys = await mysqlConnection.query(`
        SELECT DISTINCT REFERENCED_TABLE_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_NAME = ?
          AND REFERENCED_TABLE_NAME IS NOT NULL
      `, [tableName]);

      graph.set(tableName, new Set(foreignKeys.map((fk) => fk.REFERENCED_TABLE_NAME)));
    }

    const order: string[] = [];
    const visited = new Set<string>();
    const visiting = new Set<string>();

    const dfs = async (tableName: string) => {
      if (visited.has(tableName)) return;
      if (visiting.has(tableName)) throw new Error(`Circular dependency detected: ${tableName}`);

      visiting.add(tableName);
      for (const dependency of graph.get(tableName) || []) {
        await dfs(dependency);
      }
      visiting.delete(tableName);
      visited.add(tableName);
      order.push(tableName);
    };

    for (const tableName of tableNames) {
      await dfs(tableName);
    }

    return order;
  }

  private async insertBatch(transactionalEntitymanager: any, tableName: string, columns: any[], data: any[]): Promise<void> {
    const columnNames = columns.map(col => `"${col.COLUMN_NAME}"`).join(', ');
    let valuesSQL = '';
    for (const row of data) {
      const values = columns.map(col => formatValue(row[col.COLUMN_NAME], col['DATA_TYPE'].toLowerCase()));
      valuesSQL += `(${values.join(', ')}), `;
    }
    valuesSQL = valuesSQL.trim().slice(0, -1);
    await transactionalEntitymanager.query(`INSERT INTO "${tableName}" (${columnNames}) VALUES ${valuesSQL}`);
  }

  async migrateData(): Promise<{ success: boolean; message: string }> {
    const mysqlConnection = new DataSource({
      ...(mysqlConfig as DataSourceOptions),
      name: 'mysql',
      extra: {
        connectionLimit: 10,
        maxIdle: 10,
        idleTimeout: 60000,
      }
    });

    const postgresConnection = new DataSource({
      ...(postgresConfig as DataSourceOptions),
      name: 'postgres',
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

      for (const tableName of tableOrder) {
        console.log(`Analyzing table: ${tableName}`);
        
        await postgresConnection.transaction(async (transactionalEntityManager) => {
          const hasForeignKeys = await this.checkForeignKeyExists(mysqlConnection, tableName);
          
          if (hasForeignKeys) {
            console.log(`Table ${tableName} has foreign key relationships, will handle dependencies`);
            await this.migrateTableWithForeignKeys(mysqlConnection, transactionalEntityManager, tableName, progress);
          } else {
            console.log(`Table ${tableName} has no foreign keys, proceeding with direct migration`);
            await this.migrateTableWithoutConstraints(mysqlConnection, transactionalEntityManager, tableName, progress);
          }
        });
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
    progress: any
  ): Promise<void> {
    const columns = await mysqlConnection.query(`
      SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA, COLUMN_DEFAULT, NUMERIC_PRECISION, NUMERIC_SCALE
      FROM INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_NAME = ?
    `, [tableName]);

    let createTableSQL = `CREATE TABLE "${tableName}" (`;
    const columnDefinitions = columns.map((column) => {
      let type = column['DATA_TYPE'].toLowerCase();

      switch (type) {
        case 'int':
          type = column.EXTRA === 'auto_increment' ? 'SERIAL' : 'INTEGER';
          break;
        case 'bigint':
          type = column.EXTRA === 'auto_increment' ? 'BIGSERIAL' : 'BIGINT';
          break;
        case 'datetime':
          type = 'TIMESTAMP';
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
                definition += ` DEFAULT CURRENT_TIMESTAMP`;
                break;
              case 'NUMERIC':
                definition += ' DEFAULT 0';
                break;
            }
          }
        }
      }

      if (column.COLUMN_DEFAULT !== null && !['SERIAL', 'BIGSERIAL'].includes(type)) {
        definition += ` DEFAULT ${column.COLUMN_DEFAULT}`;
      }

      if (column.COLUMN_KEY === 'PRI' && !['SERIAL', 'BIGSERIAL'].includes(type)) {
        definition += ' PRIMARY KEY';
      }

      return definition;
    });

    createTableSQL += columnDefinitions.join(', ') + ')';

    await transactionalEntityManager.query(`DROP TABLE IF EXISTS "${tableName}" CASCADE`);
    await transactionalEntityManager.query(createTableSQL);

    await this.migrateTableData(mysqlConnection, transactionalEntityManager, tableName, columns, progress);

    const foreignKeys = await mysqlConnection.query(`
      SELECT 
        COLUMN_NAME,
        REFERENCED_TABLE_NAME,
        REFERENCED_COLUMN_NAME
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
      WHERE TABLE_NAME = ? 
        AND REFERENCED_TABLE_NAME IS NOT NULL
    `, [tableName]);

    for (const fk of foreignKeys) {
      try {
        await transactionalEntityManager.query(`
          ALTER TABLE "${tableName}" 
          ADD CONSTRAINT "FK_${tableName}_${fk.COLUMN_NAME}" 
          FOREIGN KEY ("${fk.COLUMN_NAME}") 
          REFERENCES "${fk.REFERENCED_TABLE_NAME}" ("${fk.REFERENCED_COLUMN_NAME}")
        `);
      } catch (err) {
        console.warn(`Failed to add foreign key for ${tableName}.${fk.COLUMN_NAME}:`, err.message);
      }
    }
  }

  private async migrateTableWithoutConstraints(
    mysqlConnection: DataSource, 
    transactionalEntityManager: any,
    tableName: string,
    progress: any
  ): Promise<void> {
    const columns = await mysqlConnection.query(`
      SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA, COLUMN_DEFAULT, NUMERIC_PRECISION, NUMERIC_SCALE
      FROM INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_NAME = ?
    `, [tableName]);

    let createTableSQL = `CREATE TABLE "${tableName}" (`;
    const columnDefinitions = columns.map((column) => {
      let type = column['DATA_TYPE'].toLowerCase();

      switch (type) {
        case 'int':
          type = column.EXTRA === 'auto_increment' ? 'SERIAL' : 'INTEGER';
          break;
        case 'bigint':
          type = column.EXTRA === 'auto_increment' ? 'BIGSERIAL' : 'BIGINT';
          break;
        case 'datetime':
          type = 'TIMESTAMP';
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
                definition += ` DEFAULT CURRENT_TIMESTAMP`;
                break;
              case 'NUMERIC':
                definition += ' DEFAULT 0';
                break;
            }
          }
        }
      }

      if (column.COLUMN_DEFAULT !== null && !['SERIAL', 'BIGSERIAL'].includes(type)) {
        definition += ` DEFAULT ${column.COLUMN_DEFAULT}`;
      }

      if (column.COLUMN_KEY === 'PRI' && !['SERIAL', 'BIGSERIAL'].includes(type)) {
        definition += ' PRIMARY KEY';
      }

      return definition;
    });

    createTableSQL += columnDefinitions.join(', ') + ')';

    await transactionalEntityManager.query(`DROP TABLE IF EXISTS "${tableName}" CASCADE`);
    await transactionalEntityManager.query(createTableSQL);

    const [{ count }] = await mysqlConnection.query(
      `SELECT COUNT(*) as count FROM ${tableName}`,
    );
    
    console.log(`Total records in ${tableName}: ${count}`);

    const totalBatches = Math.ceil(count / this.BATCH_SIZE);
    console.log(`Will process in ${totalBatches} batches`);

    let startBatch = 0;
    if (progress && progress.tableName === tableName) {
      startBatch = Math.floor(progress.lastOffset / this.BATCH_SIZE);
    }

    let totalMigrated = startBatch * this.BATCH_SIZE;
    const startTime = Date.now();

    for (let batch = startBatch; batch < totalBatches; batch++) {
      const offset = batch * this.BATCH_SIZE;
      
      if (batch % 10 === 0) {
        global.gc?.();
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      console.log(`Processing batch ${batch + 1}/${totalBatches} (offset: ${offset})`);

      const data = await mysqlConnection.query(
        `SELECT * FROM ${tableName} LIMIT ? OFFSET ?`,
        [this.BATCH_SIZE, offset],
      );

      if (data.length > 0) {
        await this.insertBatch(transactionalEntityManager, tableName, columns, data);

        totalMigrated += data.length;
        const elapsedMinutes = (Date.now() - startTime) / 60000;
        const remainingRecords = count - totalMigrated;
        const estimatedMinutes = (remainingRecords * elapsedMinutes) / totalMigrated;

        console.log(`Progress: ${totalMigrated}/${count} (${Math.round(totalMigrated/count*100)}%)`);
        console.log(`Estimated time remaining: ${Math.round(estimatedMinutes)} minutes`);

        await this.saveProgress(tableName, offset + data.length);
      }
    }
  }

  private async migrateTableData(
      mysqlConnection: DataSource,
      transactionalEntityManager: any,
      tableName: string,
      columns: any[],
      progress: any
    ): Promise<void> {
      const [{ count }] = await mysqlConnection.query(
        `SELECT COUNT(*) as count FROM ${tableName}`,
      );
      
      console.log(`Total records in ${tableName}: ${count}`);
  
      const totalBatches = Math.ceil(count / this.BATCH_SIZE);
      console.log(`Will process in ${totalBatches} batches`);
  
      let startBatch = 0;
      if (progress && progress.tableName === tableName) {
        startBatch = Math.floor(progress.lastOffset / this.BATCH_SIZE);
      }
  
      let totalMigrated = startBatch * this.BATCH_SIZE;
      const startTime = Date.now();
  
      for (let batch = startBatch; batch < totalBatches; batch++) {
        const offset = batch * this.BATCH_SIZE;
        
        if (batch % 10 === 0) {
          global.gc?.();
          await new Promise(resolve => setTimeout(resolve, 100));
        }
  
        console.log(`Processing batch ${batch + 1}/${totalBatches} (offset: ${offset})`);
  
        const data = await mysqlConnection.query(
          `SELECT * FROM ${tableName} LIMIT ? OFFSET ?`,
          [this.BATCH_SIZE, offset],
        );
  
        if (data.length > 0) {
          await this.insertBatch(transactionalEntityManager, tableName, columns, data);
  
          totalMigrated += data.length;
          const elapsedMinutes = (Date.now() - startTime) / 60000;
          const remainingRecords = count - totalMigrated;
          const estimatedMinutes = (remainingRecords * elapsedMinutes) / totalMigrated;
  
          console.log(`Progress: ${totalMigrated}/${count} (${Math.round(totalMigrated/count*100)}%)`);
          console.log(`Estimated time remaining: ${Math.round(estimatedMinutes)} minutes`);
  
          await this.saveProgress(tableName, offset + data.length);
        }
      }
    }
}