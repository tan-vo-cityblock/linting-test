import dotenv from 'dotenv';
dotenv.config();

// Config file for Knex
const config = {
  ext: 'ts',
  development: {
    client: 'pg',
    connection: {
      application_name: 'quality measure dev',
      database: 'quality_measure',
      host: '127.0.0.1',
      timezone: 'UTC',
      user: process.env.DB_USER || 'root',
    },
    migrations: {
      directory: __dirname + '/migrations',
      extension: 'ts',
    },
    pool: {
      acquireTimeoutMillis: 10000,
      idleTimeoutMillis: 1000,
      max: 50,
      min: 1,
    },
  },
  staging: {
    client: 'pg',
    connection: {
      application_name: 'quality measure staging',
      database: 'quality_measure',
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      timezone: 'UTC',
    },
    migrations: {
      directory: __dirname + '/migrations',
      extension: 'ts',
    },
    pool: {
      acquireTimeoutMillis: 10000,
      idleTimeoutMillis: 1000,
      max: 50,
      min: 1,
    },
  },
  production: {
    client: 'pg',
    connection: {
      application_name: 'quality measure prod',
      database: 'quality_measure',
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      timezone: 'UTC',
    },
    migrations: {
      directory: __dirname + '/migrations',
      extension: 'ts',
    },
    pool: {
      acquireTimeoutMillis: 10000,
      idleTimeoutMillis: 1000,
      max: 50,
      min: 1,
    },
  },
  test: {
    client: 'pg',
    connection: {
      application_name: 'quality measure test',
      database: 'quality_measure',
      host: '127.0.0.1',
      timezone: 'UTC',
      user: process.env.DB_USER || 'root',
    },
    migrations: {
      directory: __dirname + '/migrations',
      extension: 'ts',
    },
    pool: {
      acquireTimeoutMillis: 10000,
      idleTimeoutMillis: 1000,
      max: 2,
      min: 1,
    },
    seeds: {
      directory: './seeds/test',
    },
  },
};

// For the migration script
module.exports = config;
