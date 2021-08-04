
{{
  config(
    materialized='view'
  )
}}


with source as (

    select * from {{ source('mrt_commons_docker', 'telecom') }}

    where patientId is not null

),

final as (

    select
        STRUCT(
           [callIdUser] as ids,
           'callIdUser' as idField,
           'cityblock-analytics' as project,
           'mrt_commons' as dataset,
           'telecom' as table
          ) as source,

        case
          when direction = 'fromUser' then
            STRUCT(
               'userId' as `type`,
               userId as `key`,
               STRING(NULL) as `display`
             )
          else
             STRUCT(
               'patientId' as `type`,
               patientId as `key`,
               STRING(NULL) as `display`
             )
        end
        as subject,

        'calls' as verb,

        case
          when direction = 'toUser' then
            STRUCT(
               'userId' as `type`,
               userId as `key`,
               STRING(NULL) as `display`
             )
          else
            STRUCT(
             'patientId' as `type`,
             patientId as `key`,
             STRING(NULL) as `display`
            )
        end
        as directObject,

        STRUCT(
          STRING(NULL) as `type`,
          STRING(NULL) as `key`
        ) as indirectObject,

        STRUCT(
          STRING(NULL) as `type`,
          STRING(NULL) as `key`
        ) as prepositionalObject,

        STRUCT(
          cast(providerCreatedAtUser as TIMESTAMP) as createdAt,
          TIMESTAMP(NULL) as completedAt,
          TIMESTAMP(NULL) as manualEventAt,
          TIMESTAMP(NULL) as receivedAt
        ) as timestamp,

        [
          STRUCT(
            STRING(NULL) as `type`,
            STRING(NULL) as `key`
          )
        ] as purposes,

        [
          STRUCT(
            'callStatus' as `type`,
             callStatusFinal as `key`
          ),
          STRUCT(
            'transferredToIVR' as `type`,
             cast(callStatusDept is not null as STRING) as `key`
          ),
          STRUCT(
            'callDuration' as `type`,
             cast(duration as STRING) as `key`
          )
        ] as outcomes

    from source

)

select * from final
