import env from '../config';

const pool = {
  acquireTimeoutMillis: 10000,
  idleTimeoutMillis: 1000,
  max: 50,
  min: 1,
};

const config = {
  ext: 'ts',
  development: {
    client: 'pg',
    connection: {
      application_name: 'health events service dev',
      database: env.DB_NAME,
      host: env.DB_HOST,
      timezone: 'UTC',
      user: env.DB_USER || 'root',
    },
    migrations: {
      directory: __dirname + '/migrations',
      extension: 'ts',
    },
    pool,
    seeds: {
      directory: './seeds/staging',
    },
  },
  staging: {
    client: 'pg',
    connection: {
      application_name: 'health events service staging',
      database: env.DB_NAME,
      host: env.DB_HOST,
      user: env.DB_USER,
      password: env.DB_PASSWORD,
      timezone: 'UTC',
    },
    migrations: {
      directory: __dirname + '/migrations',
      extension: 'ts',
    },
    pool,
    seeds: {
      directory: './seeds/staging',
    },
  },
  production: {
    client: 'pg',
    connection: {
      application_name: 'health events service prod',
      database: env.DB_NAME,
      host: env.DB_HOST,
      user: env.DB_USER,
      password: env.DB_PASSWORD,
      timezone: 'UTC',
    },
    migrations: {
      directory: __dirname + '/migrations',
      extension: 'ts',
    },
    pool,
  },
  test: {
    client: 'pg',
    connection: {
      application_name: 'health events service test',
      database: 'health_events_service_test',
      host: env.DB_HOST,
      timezone: 'UTC',
      user: env.DB_USER || 'root',
      password: env.DB_PASSWORD,
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
  },
};

module.exports = config;
