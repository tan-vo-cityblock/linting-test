export const up = `
DROP MATERIALIZED VIEW IF EXISTS hie_message_signal;

CREATE MATERIALIZED VIEW hie_message_signal AS (
  WITH hie_columns AS (
    SELECT 
      hie_message."messageId" AS "messageId", 
      hie_message."patientId" AS "patientId", 
      hie_message."eventType" AS "eventType",
      CASE
      WHEN (hie_message."eventType" = 'Notes.New') 
      THEN (
        CASE
        WHEN coalesce(regexp_replace(hie_message."payload"#>>'{Note,Components,0,Value}','"',''),regexp_replace(hie_message."payload"#>>'{Meta,EventType}','"','')) = 'Arrival'
        THEN 'Admit'
        ELSE coalesce(regexp_replace(hie_message."payload"#>>'{Note,Components,0,Value}','"',''),regexp_replace(hie_message."payload"#>>'{Meta,EventType}','"',''))
        END
      ) ELSE (
        cast(json_extract_path_text(hie_message."payload" :: json, 'Meta', 'EventType') as varchar)
      )
      END as "metaEventType",
      CASE
        WHEN (
          hie_message."eventType" = 'Notes.New'
          AND hie_message."payload"#>>'{Visit,VisitDateTime}' IS NOT NULL
        ) THEN (
          (hie_message."payload"#>>'{Visit,VisitDateTime}')::timestamptz
        )
        WHEN (
          hie_message."eventType" = 'Notes.New'
          AND hie_message."payload"#>>'{Visit,VisitDateTime}' IS NULL
          AND hie_message."payload"#>>'{Note,FileContents}' IS NOT NULL
        ) THEN (
          to_timestamp(substring((hie_message."payload"#>>'{Note,FileContents}'), '(?i) Admission on ([a-z ]+[0-9 ]+:[0-9 ]+[pa]m)'), 'Mon DD YYYY HH:MIam')
        )
        WHEN (
          hie_message."eventType" IN ('PatientAdmin.Discharge', 'PatientAdmin.Arrival')
          AND (hie_message."payload"#>>'{Meta,Source,Name}') IN ('PatientPing Source (p)', 'PatientPing Source (s)', 'CRISP [PROD] ADT Source (p)')
          AND hie_message."payload"#>>'{Visit,VisitDateTime}' IS NOT NULL
        ) THEN (
          (hie_message."payload"#>>'{Visit,VisitDateTime}')::timestamptz
        )
        ELSE (
          (hie_message."payload"#>>'{Meta,EventDateTime}')::timestamptz
        )
        END as "eventDateTime"
    FROM hie_message
  ),
  with_is_readmission as (
    select 
      *, 
      case when 
      hie_columns."metaEventType" != 'Discharge'
      and
      hie_columns."eventDateTime" :: date
      -
      lag(hie_columns."eventDateTime" :: date, 1) over (
        partition by "patientId" 
        order by 
          hie_columns."eventDateTime" :: date
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

CREATE UNIQUE INDEX hie_message_id_index ON hie_message_signal("messageId");
`;

export const down = `
DROP MATERIALIZED VIEW IF EXISTS hie_message_signal;

CREATE MATERIALIZED VIEW hie_message_signal AS (
  WITH hie_columns AS (
    SELECT 
      hie_message."messageId" AS "messageId", 
      hie_message."patientId" AS "patientId", 
      hie_message."eventType" AS "eventType",
      cast(
        json_extract_path_text(
          payload :: json, 'Meta', 'EventType'
        ) as varchar
      ) as "metaEventType",
      CASE
        WHEN (
          hie_message."eventType" = 'Notes.New'
          AND hie_message."payload"#>>'{Visit,VisitDateTime}' IS NOT NULL
        ) THEN (
          (hie_message."payload"#>>'{Visit,VisitDateTime}')::timestamptz
        )
        WHEN (
          hie_message."eventType" = 'Notes.New'
          AND hie_message."payload"#>>'{Visit,VisitDateTime}' IS NULL
          AND hie_message."payload"#>>'{Note,FileContents}' IS NOT NULL
        ) THEN (
          to_timestamp(substring((hie_message."payload"#>>'{Note,FileContents}'), '(?i) Admission on ([a-z ]+[0-9 ]+:[0-9 ]+[pa]m)'), 'Mon DD YYYY HH:MIam')
        )
        WHEN (
          hie_message."eventType" IN ('PatientAdmin.Discharge', 'PatientAdmin.Arrival')
          AND (hie_message."payload"#>>'{Meta,Source,Name}') IN ('PatientPing Source (p)', 'PatientPing Source (s)', 'CRISP [PROD] ADT Source (p)')
          AND hie_message."payload"#>>'{Visit,VisitDateTime}' IS NOT NULL
        ) THEN (
          (hie_message."payload"#>>'{Visit,VisitDateTime}')::timestamptz
        )
        ELSE (
          (hie_message."payload"#>>'{Meta,EventDateTime}')::timestamptz
        )
        END as "eventDateTime"
    FROM hie_message
  ),
  with_is_readmission as (
    select 
      *, 
      case when 
      hie_columns."metaEventType" != 'Discharge'
      and
      hie_columns."eventDateTime" :: date
      -
      lag(hie_columns."eventDateTime" :: date, 1) over (
        partition by "patientId" 
        order by 
          hie_columns."eventDateTime" :: date
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

CREATE UNIQUE INDEX hie_message_id_index ON hie_message_signal("messageId");
`;
