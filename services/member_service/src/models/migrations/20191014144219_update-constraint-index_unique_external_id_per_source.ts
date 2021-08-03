import * as Knex from 'knex';


export async function up(knex: Knex): Promise<any> {
    
    await knex.schema.table('member_datasource_identifier', table => {
        table.dropUnique([], 'index_unique_external_id_per_source');
    });

    await knex.raw('create UNIQUE INDEX index_unique_external_id_per_source on member_datasource_identifier ("datasourceId", "externalId")  WHERE "deletedAt" IS NULL');
}


export async function down(knex: Knex): Promise<any> {

    await knex.schema.table('member_datasource_identifier', table => {
        table.dropIndex([], 'index_unique_external_id_per_source');
        table.unique(['datasourceId', 'externalId', 'deletedAt'], 'index_unique_external_id_per_source');
    });    
}

