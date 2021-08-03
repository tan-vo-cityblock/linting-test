import dotenv from 'dotenv';
dotenv.config();

interface IEnvironmentVariables extends NodeJS.ProcessEnv {
  DB_HOST: string;
  DB_NAME: string;
  DB_PASSWORD: string | undefined;
  DB_PORT: string | undefined;
  DB_SSL: string | undefined;
  DB_USER: string | undefined;
  NODE_ENV: 'development' | 'test' | 'staging' | 'production';
  GCP_CREDS: string;
}

export default {
  DB_HOST: '127.0.0.1',
  DB_NAME: 'health_events_service',
  NODE_ENV: 'development',
  DB_PASSWORD: undefined,
  DB_PORT: undefined,
  DB_SSL: undefined,
  DB_USER: undefined,
  GCP_CREDS: '',
  ...process.env,
} as IEnvironmentVariables;
