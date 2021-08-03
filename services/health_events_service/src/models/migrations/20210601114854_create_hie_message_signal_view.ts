import { Knex } from 'knex';

const VIEW_NAME = 'hie_message_signal';

const TRIGGER_NAME = 'hie_message_signal_trigger';

export async function up(knex: Knex): Promise<any> {
  await knex.raw(`
    CREATE MATERIALIZED VIEW hie_message_signal AS (
        with hie_columns as (
          select 
            hie_message."messageId" as "messageId", 
            hie_message."patientId" as "patientId", 
            hie_message."eventType" as "eventType", 
            cast(
              json_extract_path_text(
                payload :: json, 'Meta', 'EventType'
              ) as varchar
            ) as "metaEventType", 
            hie_message."eventDateTime" as "eventDateTime" 
          from 
            hie_message
        ), 
        with_is_readmission as (
          select 
            *, 
            case when 
            hie_columns."metaEventType" != 'Discharge'
            and
            DATE_PART(
              'day', hie_columns."eventDateTime" :: date
            ) - DATE_PART(
              'day', 
              lag(hie_columns."eventDateTime" :: date, 1) over (
                partition by "patientId" 
                order by 
                  hie_columns."eventDateTime" :: date
              )
            ) 
            between 0 
            and 30
            and lag(hie_columns."metaEventType", 1) over (
              partition by "patientId" 
              order by 
                hie_columns."eventDateTime" :: date
            ) = 'Discharge' then true else false end as "isReadmission" 
          from 
            hie_columns 
          group by 
            1, 
            2, 
            3, 
            4, 
            5
        ) 
        SELECT 
          * 
        FROM 
          with_is_readmission
      );

        CREATE OR REPLACE FUNCTION hie_message_signal_refresh() 
        RETURNS TRIGGER LANGUAGE plpgsql
        AS $$ 
        BEGIN
        REFRESH MATERIALIZED VIEW ${VIEW_NAME};
        RETURN NULL;
        END $$;
        
        DROP TRIGGER IF EXISTS  ${TRIGGER_NAME}
        ON hie_message;

        CREATE TRIGGER ${TRIGGER_NAME} AFTER INSERT 
        on hie_message
        EXECUTE PROCEDURE hie_message_signal_refresh();
        
        `);
}

export async function down(knex: Knex): Promise<any> {
  await knex.raw(`
    DROP MATERIALIZED VIEW IF EXISTS ${VIEW_NAME};
    DROP FUNCTION IF EXISTS hie_message_signal_refresh CASCADE;
    `);
}
