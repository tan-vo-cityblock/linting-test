import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  const query = `INSERT INTO
    mrn
  SELECT
    mdi."id" as "id",
    "externalId" AS "mrn",
    "name"::mrn_name,
    mdi."createdAt",
    mdi."updatedAt",
    mdi."deletedAt",
    mdi."deletedReason"
  FROM
    member_datasource_identifier mdi
  LEFT JOIN
    datasource
  ON
    datasource."id" = "datasourceId"
  WHERE
    "datasourceId" = 2
    OR "datasourceId" = 4;`;
  await knex.raw(query);
}

export async function down(knex: Knex): Promise<any> {
  await knex('mrn').del();
}