import { DataSourceOptions } from 'typeorm';

export const mysqlConfig: DataSourceOptions = {
  type: 'mysql',
  host: 'localhost',
  port: 3306,
  username: 'root',
  password: '1',
  database: 'store',
  synchronize: false,
  extra: {
    connectionLimit: 10,
    maxIdle: 10,
    idleTimeout: 60000,
  },
};

export const postgresConfig: DataSourceOptions = {
  type: 'postgres',
  host: 'localhost',
  port: 5432,
  username: 'postgres',
  password: '1',
  database: 'laravel',
  synchronize: false,
  dropSchema: true,
  extra: {
    max: 20,
    maxUses: 7500,
    idleTimeoutMillis: 30000,
    timezone: 'UTC', // +05:00 o'rniga UTC qo'yamiz
    session_replication_role: 'replica',
    statement_timeout: 0,
    lock_timeout: 0,
    idle_in_transaction_session_timeout: 0,
    application_name: 'migration',
  },
  logging: ['query', 'error', 'schema'],
  maxQueryExecutionTime: 300000, // 5 daqiqa
  entitySkipConstructor: true,
  migrationsRun: false,
  migrationsTransactionMode: 'none',
  installExtensions: false,
  ssl: false,
  cache: false,
};