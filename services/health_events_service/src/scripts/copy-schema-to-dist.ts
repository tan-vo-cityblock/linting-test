import fs from 'fs';
import path from 'path';

const main = () => {
  const schemaFileSource = path.join(__dirname, '../graphql/schema.graphql');
  const schemaFileTarget = path.join(__dirname, '../../dist/src/graphql/schema.graphql');
  fs.copyFile(schemaFileSource, schemaFileTarget, (err: Error | null) => {
    if (err) {
      throw err;
    }
  });
};

main();
