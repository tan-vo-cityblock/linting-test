// @ts-ignore Error is thrown when trying to build.
import type { Config } from '@jest/types';

const config: Config.InitialOptions = {
  verbose: true,
  rootDir: './src',
  setupFilesAfterEnv: ['<rootDir>/../jest-setup.ts'],
  preset: 'ts-jest',
  testEnvironment: 'node',
  clearMocks: true,
};

export default config;
