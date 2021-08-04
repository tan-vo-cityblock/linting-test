import { Knex } from 'knex';

const TRIGGER_NAME = `hie_message_signal_trigger`;

export async function up(knex: Knex): Promise<void> {
  await knex.raw(`
    DROP TRIGGER IF EXISTS ${TRIGGER_NAME} ON hie_message;
    DROP FUNCTION IF EXISTS hie_message_signal_refresh CASCADE;
  `);
  return Promise.resolve();
}

export async function down(knex: Knex): Promise<void> {
  await knex.raw(`
    CREATE OR REPLACE FUNCTION hie_message_signal_refresh() 
    RETURNS TRIGGER LANGUAGE plpgsql
    AS $$ 
    BEGIN
    REFRESH MATERIALIZED VIEW hie_message_signal;
    RETURN NULL;
    END $$;
    
    DROP TRIGGER IF EXISTS  ${TRIGGER_NAME}
    ON hie_message;

    CREATE TRIGGER ${TRIGGER_NAME} AFTER INSERT 
    on hie_message
    EXECUTE PROCEDURE hie_message_signal_refresh();
  `);
  return Promise.resolve();
}
