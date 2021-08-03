import * as Knex from "knex";

const oldMaritalStatusTypes = `
    'unknown',
    'divorced',
    'legallySeparated',
    'neverMarried',
    'domesticPartner',
    'unmarried',
    'widowed'
`;

const newMaritalStatusTypes = `
    'currentlyMarried'
`;

export async function up(knex: Knex): Promise<any> {
    return knex.raw(`
        ALTER TYPE marital_status RENAME TO _marital_status;
        CREATE TYPE marital_status AS enum (${oldMaritalStatusTypes}, ${newMaritalStatusTypes});
        ALTER TABLE member_demographics RENAME COLUMN "maritalStatus" to "_maritalStatus";
        ALTER TABLE member_demographics ADD "maritalStatus" marital_status not null default 'unknown';
        UPDATE member_demographics SET "maritalStatus" = "_maritalStatus"::text::marital_status;
        ALTER TABLE member_demographics DROP column "_maritalStatus";
        DROP type _marital_status;
    `);
}

export async function down(knex: Knex): Promise<any> {
    return knex.raw(`
        ALTER TYPE marital_status RENAME TO _marital_status;
        CREATE TYPE marital_status AS enum (${oldMaritalStatusTypes});
        ALTER TABLE member_demographics RENAME COLUMN "maritalStatus" to "_maritalStatus";
        ALTER TABLE member_demographics ADD "maritalStatus" marital_status not null default 'unknown';
        UPDATE member_demographics SET "maritalStatus" = "_maritalStatus"::text::marital_status;
        ALTER TABLE member_demographics DROP column "_maritalStatus";
        DROP type _marital_status;
    `);
}
