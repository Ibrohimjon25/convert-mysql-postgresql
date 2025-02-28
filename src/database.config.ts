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
  }
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
    timezone: '+05:00',
    // PostgreSQL specific settings for foreign keys
    session_replication_role: 'replica',
    // Add these settings
    statement_timeout: 0,
    lock_timeout: 0,
    idle_in_transaction_session_timeout: 0,
    application_name: 'migration'
  },
  logging: ["query", "error", "schema"],
  maxQueryExecutionTime: 300000, // Increased to 5 minutes
  entitySkipConstructor: true,
  migrationsRun: false,
  migrationsTransactionMode: "none", // Changed to handle constraints better
  installExtensions: false,
  ssl: false,
  cache: false
};
