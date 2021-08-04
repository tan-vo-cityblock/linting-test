import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
    await knex.schema.table('member_datasource_identifier', table => {
        table.enu('rank', ['primary', 'secondary', 'tertiary'],  { useNative: true, enumName: 'insurance_rank' });
        table.date('spanDateStart');
        table.date('spanDateEnd');
    });
}

export async function down(knex: Knex): Promise<any> {
    await knex.schema.table('member_datasource_identifier', table => {
        table.dropColumn('rank');
        table.dropColumn('spanDateStart');
        table.dropColumn('spanDateEnd');
    });
    await knex.raw('DROP TYPE insurance_rank');
}
