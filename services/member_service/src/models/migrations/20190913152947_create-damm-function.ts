import * as Knex from 'knex';

// The logic in this migration is adapted from https://github.com/fusiongyro/damm
export async function up(knex: Knex): Promise<any> {
  return knex.schema.hasTable('damm_matrix').then(async exists => {
    if (!exists) {
      await knex.schema.createTable('damm_matrix', table => {
        table.specificType('i', 'smallint');
        table.specificType('j', 'smallint');
        table.specificType('v', 'smallint');
        table.primary(['i', 'j']);
      });

      const dammMatrix = [
        [0, 3, 1, 7, 5, 9, 8, 6, 4, 2],
        [7, 0, 9, 2, 1, 5, 4, 8, 6, 3],
        [4, 2, 0, 6, 8, 7, 1, 3, 5, 9],
        [1, 7, 5, 0, 9, 8, 3, 4, 2, 6],
        [6, 1, 2, 3, 0, 4, 5, 9, 7, 8],
        [3, 6, 7, 4, 2, 0, 9, 5, 8, 1],
        [5, 8, 6, 9, 7, 2, 0, 1, 3, 4],
        [8, 9, 4, 5, 3, 6, 2, 0, 1, 7],
        [9, 4, 3, 8, 6, 1, 7, 2, 0, 5],
        [2, 5, 8, 1, 4, 3, 6, 7, 9, 0],
      ];
      
      // This weird concatenation is just a flatMap, which is annoyingly not available on arrays
      // before ES2019.
      const entries = [].concat(...dammMatrix.map((row, i) => row.map((v, j) => ({ i, j, v }))));
      await knex('damm_matrix').insert(entries);

      // Function to compute the Damm check digit for a given number.
      await knex.raw(`
        CREATE OR REPLACE FUNCTION damm_check_digit(bigint) RETURNS smallint AS $$
          WITH RECURSIVE prev AS
            (SELECT
              string_to_array($1::varchar, null)::smallint[] AS digits,
              0::smallint AS interim,
              1 as i
            UNION ALL
            SELECT prev.digits, v AS interim, prev.i+1
            FROM prev
            JOIN damm_matrix dm ON (dm.i,dm.j) = (prev.interim, prev.digits[prev.i]))
          SELECT interim AS code
          FROM prev
          ORDER BY i DESC
          LIMIT 1
        $$ LANGUAGE SQL;
      `);

      // Function to check if the number up is a valid Damm code (meaning the last digit is a
      // check digit that verifies the preceeding digits).
      await knex.raw(`
        CREATE OR REPLACE FUNCTION valid_damm_code(bigint) RETURNS boolean AS $$
          SELECT damm_check_digit($1 / 10) = ($1 % 10)
        $$ LANGUAGE SQL;
      `);

      // Function to compute the Damm check digit for a given number and append it to the
      // number, yielding a valid Damm code.
      await knex.raw(`
        CREATE OR REPLACE FUNCTION generate_damm(bigint) RETURNS bigint AS $$
          SELECT $1 * 10 + damm_check_digit($1)
        $$ LANGUAGE SQL;
      `);

      // A nextval() replacement that produces sequential Damm codes from a sequence instead of
      // sequential integers.
      await knex.raw(`
        CREATE OR REPLACE FUNCTION nextdamm(varchar) RETURNS bigint AS $$
          SELECT generate_damm(nextval($1))
        $$ LANGUAGE SQL;
      `);
    }
  });
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema
    .dropTableIfExists('damm_matrix')
    .raw('DROP FUNCTION IF EXISTS damm_check_digit')
    .raw('DROP FUNCTION IF EXISTS valid_damm_code')
    .raw('DROP FUNCTION IF EXISTS generate_damm')
    .raw('DROP FUNCTION IF EXISTS nextdamm');
}
