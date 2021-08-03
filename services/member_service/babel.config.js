module.exports = {
    presets: [
      [
        '@babel/env',
        {
          targets: {
            browsers: ['last 2 Chrome versions'],
          },
        },
      ],
      '@babel/typescript',
    ],
    plugins: [
      'const-enum',
      '@babel/proposal-class-properties',
      '@babel/proposal-object-rest-spread',
    ],
  };