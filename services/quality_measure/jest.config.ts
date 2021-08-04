const babelConfig = require('./babel.config.js');

module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'jsdom',
  reporters: ['default', 'jest-junit'],
  transform: {
    '\\.(gql|graphql)$': 'jest-transform-graphql',
    '^.+\\.tsx?$': 'ts-jest',
  },
  moduleNameMapper: {
    '\\.(css)$': 'identity-obj-proxy',
  },
  testEnvironmentOptions: {},
  testMatch: [
    '<rootDir>/models/**/?(*.)(spec|test).ts?(x)',
    '<rootDir>/util/**/?(*.)(spec|test).ts?(x)',
  ],
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
  setupFiles: ['./browser-mocks.js', './enzyme-setup.js'],
  setupFilesAfterEnv: ['./jest-setup.js'],
  coverageReporters: ['json'],
  coverageDirectory: './coverage/',
  collectCoverageFrom: [
    'models/**/*.{ts,tsx}',
    'util/**/*.{ts,tsx}',
    '!**/node_modules/**',
  ],
  globals: {
    VERSION: 'v0.0.1',
    'ts-jest': {
      babelConfig,
      diagnostics: false,
      isolatedModules: true,
      tsConfig: 'tsconfig.test.json',
    },
  },
};
