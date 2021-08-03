import * as Knex from 'knex';

export async function up(knex: Knex): Promise<any> {
  await knex.schema.createTable('member_demographics', (table) => {
    table.uuid('id').unique().defaultTo(knex.raw('uuid_generate_v4()'));
    table
      .uuid('memberId')
      .references('id')
      .inTable('member')
      .onUpdate('CASCADE')
      .onDelete('RESTRICT')
      .notNullable();
    table.string('firstName').notNullable();
    table.string('middleName');
    table.string('lastName').notNullable();
    table.string('dateOfBirth').notNullable();
    table.string('dateOfDemise');
    table.enu('sex', ['male', 'female'], { useNative: true, enumName: 'sex_name' }).notNullable();
    table.string('gender');
    table.string('ethnicity');
    table
      .enu(
        'maritalStatus',
        [
          'unknown',
          'divorced',
          'legallySeparated',
          'neverMarried',
          'domesticPartner',
          'unmarried',
          'widowed',
        ],
        { useNative: true, enumName: 'marital_status' },
      )
      .defaultTo('unknown')
      .notNullable();
    table.string('ssn');
    table.string('ssnLastFour');
    table.timestamp('createdAt').notNullable().defaultTo(knex.fn.now());
    table.timestamp('updatedAt').notNullable().defaultTo(knex.fn.now());
    table.timestamp('deletedAt');
    table.text('deletedReason');
    table.text('updatedBy');
    table.text('deletedBy');
    table.text('clientSource');
  });

  const demographicInfoUniquenessConstraint = knex.raw(
    'create UNIQUE INDEX index_demographic_id_per_member on member_demographics ("memberId") WHERE "deletedAt" IS NULL',
  );

  const ssnUniquenessConstraint = knex.raw(
    'create UNIQUE INDEX index_ssn_per_member on member_demographics ("ssn") WHERE "deletedAt" IS NULL',
  );

  return Promise.all([ssnUniquenessConstraint, demographicInfoUniquenessConstraint]);
}

export async function down(knex: Knex): Promise<any> {
  await knex.schema.dropTableIfExists('member_demographics');
  return Promise.all([knex.raw('DROP TYPE sex_name'), knex.raw('DROP TYPE marital_status')]);
}
