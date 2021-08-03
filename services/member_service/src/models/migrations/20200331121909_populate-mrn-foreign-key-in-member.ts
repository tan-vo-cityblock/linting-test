import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const query = `WITH
    subquery AS (
    SELECT
      "id", "memberId"
    FROM
      member_datasource_identifier
    WHERE
      ("datasourceId" = 2
        OR "datasourceId" = 4)
      AND "deletedAt" IS NULL )
  UPDATE
    member
  SET
    "mrnId" = subquery."id"
  FROM
    subquery
  WHERE
    member."id" = subquery."memberId";`;

  await knex.raw(query);
}

export async function down(knex: Knex): Promise<any> {
  await knex.table('member').update({
    mrnId: null,
  });
}
