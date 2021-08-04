

{{
  config(
    materialized='view'
  )
}}

with source as (

    select * from {{ source('commons', 'sms_message') }}

),

final as (

    select
        STRUCT(
           [id] as ids,
           'id' as idField,
           'cbh-db-mirror-prod' as project,
           'commons_mirror' as dataset,
           'sms_messages' as table
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

     'messages' as verb,

     case when direction = 'toUser' then
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
     end as directObject,

      STRUCT(
        STRING(NULL) as `type`,
        STRING(NULL) as `key`
      ) as indirectObject,

      STRUCT(
        STRING(NULL) as `type`,
        STRING(NULL) as `key`
      ) as prepositionalObject,

      STRUCT(
        cast(providerCreatedAt as TIMESTAMP) as createdAt,
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
          STRING(NULL) as `type`,
          STRING(NULL) as `key`
        )
      ] as outcomes

        from source
        where patientId is not null
)

select * from final
