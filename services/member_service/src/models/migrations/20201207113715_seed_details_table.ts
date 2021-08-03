import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  return knex.raw(`
    INSERT INTO
      member_insurance_details(
        "memberDatasourceId",
        "createdAt",
        "updatedAt",
        "spanDateStart",
        "spanDateEnd",
        "lineOfBusiness",
        "subLineOfBusiness",
        "clientSource",
        "updatedBy")
    SELECT
      mdi.id,
      mdi."createdAt",
      mdi."updatedAt",
      "spanDateStart",
      "spanDateEnd",
      "lineOfBusiness"::text::insurance_line_of_business,
      "subLineOfBusiness",
      mdi."clientSource",
      mdi."updatedBy"
    FROM
      member_insurance mdi
    INNER JOIN
      datasource ds
    ON
      ds.id = mdi."datasourceId"
   `);
}

export async function down(knex: Knex): Promise<any> {
  return knex.schema.dropTableIfExists('member_insurance_details');
}
