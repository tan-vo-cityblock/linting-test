

{{
  config(
    materialized='view'
  )
}}

with source as (

    select * from {{ ref('stg_hie_filtered') }}


),

base_event_data as (

    select
        hie.patientId,
        hie.visitNumber,
        hie.messageId,
        eventDTInstant,
        dischargeDTInstant,
        hie.eventType,
        locationName,
        visitType,
        hieSource,
        receivedAt,
        [
            STRUCT('admits' AS verb, eventDTInstant AS occuredAt, STRING(null) as dischargeDisposition),
            STRUCT('discharges' AS verb, dischargeDTInstant AS occuredAt, dischargeDisposition)
        ] as event_data

    from source as  hie

    where hie.eventType = 'Discharge'

),

split_admits_discharges as (

    select
        patientId,
        visitNumber,
        messageId,
        eventType,
        locationName,
  --       dischargeDisposition,
        visitType,
        hieSource,
        receivedAt,
        event_data.*

    from base_event_data

    cross join UNNEST(base_event_data.event_data) AS event_data

),

split_admits as (

  select *

  from split_admits_discharges

  where verb = 'admits'

),

split_discharges as (

    select *

    from split_admits_discharges

    where verb = 'discharges'

),

pure_admits as (

    select
        patientId,
        visitNumber,
        messageId,
        eventType,
        locationName,
        visitType,
        hieSource,
        receivedAt,
        'admits' as visitPart,
        eventDTInstant as occuredAt,
        STRING(NULL) as dischargeDisposition

    from source as hie

    where eventType = 'Admit'

),

indirect_objects_union as (

    select * from split_discharges

    union all

    select * from pure_admits

    union all

    (select * from split_admits where visitNumber not in (select distinct visitNumber from pure_admits))

),

admit_to_text as (

    select
        messageId,
        case
          when ARRAY_LENGTH(SPLIT(fullText, "admitted to ")) > 1 then SPLIT(fullText, "admitted to ")[ORDINAL(2)]
          else null
        end
        as facNameSentence

    from source as hie

),

message_fac_name as (

    select
        messageId,
        case
          when ARRAY_LENGTH(SPLIT(facNameSentence, " (MRN")) > 0 then SPLIT(facNameSentence, " (MRN")[ORDINAL(1)]
          else null
        end
        as parsedFacName

    from admit_to_text

),

final as (

--       select * from indirect_objects_union where visitType = 'O'

    select
      STRUCT(
          ARRAY_AGG(distinct hie.visitNumber IGNORE NULLS) as ids,
          'visitNumber' as idField, -- visitNumber
          'cityblock-data' as project,
          'medical' as dataset,
          'patient_hie_events' as table
      ) as source,

      STRUCT(
         'facility' as `type`,
         STRING(NULL) as `key`,
         coalesce(parsedFacName, locationName) as `display`
      ) as subject,

      verb,

      STRUCT(
        'patientId' as `type`,
        hie.patientId as `key`,
        STRING(NULL) as `display`
      ) as directObject,

      STRUCT(
        'setting' as `type`,
        case when visitType IN ('ED', 'Emergency') then 'ED'
             when visitType = 'IA' then 'Inpatient'
             when visitType IN ('O', 'Outpatient') then 'Outpatient'
             when visitType = 'Observation' then 'Observation'
             when visitType = 'Unknown' then null
             when visitType = 'S' then 'SNF'
        else visitType end as `key`
      ) as indirectObject,

      STRUCT(
        'market' as `type`,
        case when hieSource = 'Healthix' then 'New York City' else 'Connecticut' end as `key`
      ) as prepositionalObject,

      STRUCT(
--         case
--             when REGEXP_CONTAINS(eventType, 'Discharge') then cast(dischargeDateTime.instant as TIMESTAMP)
--             else cast(eventDateTime.instant as TIMESTAMP) end as createdAt,
        occuredAt as createdAt,
        TIMESTAMP(NULL) as completedAt,
        TIMESTAMP(NULL) as manualEventAt,
        cast(receivedAt as TIMESTAMP) as receivedAt
      ) as timestamp,

      [
        STRUCT(
          'medicalService' as `type`,
          'acute' as `key`
        )
      ] as purposes,

      case
        when REGEXP_CONTAINS(eventType, 'Discharge') and dischargeDisposition is not null then
        [
          STRUCT(
            'dischargeToSetting' as `type`,
             dischargeDisposition as `key`
          )
        ]
      end as outcomes

    from indirect_objects_union hie
    left join message_fac_name as facName
      on facName.messageId = hie.messageId

    group by
      hie.patientId,
      occuredAt,
      eventType,
      locationName,
      dischargeDisposition,
      visitType,
      verb,
      receivedAt,
      hieSource,
      hie.visitNumber,
      parsedFacName

)

select * from final
