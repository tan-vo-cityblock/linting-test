import { Knex } from 'knex';

const TABLE_NAME = 'facility_map_cache';

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTableIfNotExists(TABLE_NAME, (table) => {
    table.string('facilityName');
    table.string('mappedFacilityName');
    table.string('mappedFacilityNPI');
    table.string('mappedFacilityType');
    table.string('behavioralOrPsychFacility');
    table.string('pingSource');
    table.date('createdAt');
    table.date('lastModified');
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists(TABLE_NAME);
}
