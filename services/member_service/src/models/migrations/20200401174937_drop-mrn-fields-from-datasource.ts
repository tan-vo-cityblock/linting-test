import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  await knex.table('member_datasource_identifier').where({datasourceId: 2}).orWhere({datasourceId: 4}).del();
  await knex.table('datasource').where({id: 2}).orWhere({id: 4}).del();
}

export async function down(knex: Knex): Promise<any> {
  await knex.table('datasource').insert({ id: 2, name: 'acpny' });
  await knex.table('datasource').insert({ id: 4, name: 'elation' });

  const query = `SELECT
    mrn."id",
    member."id" AS memberId,
    mrn."mrn" AS "externalId",
    CASE
      WHEN name = 'elation' THEN 4
      ELSE 2
    END AS datasourceId,
    mrn."createdAt",
    mrn."updatedAt",
    mrn."deletedAt",
    mrn."deletedReason",
    true as "current"
  FROM
    mrn
  LEFT JOIN
    member
  ON
    mrn."id" = member."mrnId"
  WHERE
    member."id" IS NOT NULL;`;

  await knex.raw(`INSERT INTO member_datasource_identifier ${query}`);
}
