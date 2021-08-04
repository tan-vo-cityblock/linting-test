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
    '<rootDir>/app/**/?(*.)(spec|test).ts?(x)',
    '<rootDir>/server/**/?(*.)(spec|test).ts?(x)',
    '<rootDir>/scripts/**/?(*.)(spec|test).ts?(x)',
    '<rootDir>/shared/**/?(*.)(spec|test).ts?(x)',
  ],
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
  setupFiles: ['./browser-mocks.js', './enzyme-setup.js'],
  setupFilesAfterEnv: ['./jest-setup.js'],
  coverageReporters: ['json'],
  coverageDirectory: './coverage/',
  collectCoverageFrom: [
    'app/**/*.{ts,tsx}',
    'scripts/**/*.{ts,tsx}',
    'server/**/*.{ts,tsx}',
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