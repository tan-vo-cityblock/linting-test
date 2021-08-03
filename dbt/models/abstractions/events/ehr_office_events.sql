

{{
  config(
    materialized='view'
  )
}}

with source as (

    select * from {{ ref('src_patient_encounters_latest') }}

),

final as (

    select
        STRUCT(
            array_agg(distinct unnest_identifiers.ID) as ids,
            'encounter.Identifiers.ID' as idField,
            'cityblock-data' as project,
            'medical' as dataset,
            'latest_patient_encounters' as table
          ) as source,

        STRUCT(
           ANY_VALUE(IF(ARRAY_LENGTH(unnest_providers.identifiers) > 0, unnest_providers.identifiers[OFFSET(0)].idType, NULL)) as `type`,
           ANY_VALUE(IF(ARRAY_LENGTH(unnest_providers.identifiers) > 0, unnest_providers.identifiers[OFFSET(0)].id, NULL)) as `key`,
           CONCAT(unnest_providers.FirstName, ' ', unnest_providers.LastName) as `display`
        ) as subject,

        'meets' as verb,

        STRUCT(
          'patientId' as `type`,
          patient.patientId as `key`,
          STRING(NULL) as `display`
        ) as directObject,

        STRUCT(
          'setting' as `type`,
          case when REGEXP_CONTAINS(lower(encounter.Type.Name), 'home') then 'Home' else 'Clinic' end as `key`
        ) as indirectObject,

        STRUCT(
          STRING(NULL) as `type`,
          STRING(NULL) as `key`
        ) as prepositionalObject,

        STRUCT(
          coalesce(cast(encounter.DateTime.instant as TIMESTAMP), cast(encounter.DateTime.date as TIMESTAMP)) as createdAt,
          TIMESTAMP(NULL) as completedAt,
          TIMESTAMP(NULL) as manualEventAt,
          TIMESTAMP(NULL) as receivedAt
        ) as timestamp,

        case when (unnest_providers.Role.Name IN ('Internal Medicine', 'Family Medicine') or
                  (unnest_locations.Type.Name IN ('Internal Medicine', 'Family Medicine'))) then

        [
          STRUCT(
             'medicalService' as `type`,
             'primaryCare' as `key`
           ),
           STRUCT(
            'locationSpecialty' as `type`,
            unnest_locations.Type.Name as `key` -- unnest_locations.Type.Name
          ),
          STRUCT(
            'providerSpecialty' as `type`,
            unnest_providers.Role.Name as `key` -- unnest_locations.Type.Name
          ),
          STRUCT(
            'encounterType' as `type`,
            encounter.Type.Name as `key` -- unnest_locations.Type.Name
          )
        ]

        else

        [
          STRUCT(
            'locationSpecialty' as `type`,
            unnest_locations.Type.Name as `key` -- unnest_locations.Type.Name
          ),
          STRUCT(
            'providerSpecialty' as `type`,
            unnest_providers.Role.Name as `key` -- unnest_locations.Type.Name
          ),
          STRUCT(
            'encounterType' as `type`,
            encounter.Type.Name as `key` -- unnest_locations.Type.Name
          )
        ] end as purposes,

        [
          STRUCT(
             STRING(NULL) as `type`,
             STRING(NULL) as `key`
          )
        ] as outcomes


    from source,

    unnest(encounter.Providers) as unnest_providers,

    unnest(encounter.Identifiers) as unnest_identifiers,

    unnest(encounter.locations) as unnest_locations

    where encounter.Type.Name IN ('Office Visit', 'Initial Consult', 'Procedure visit',
      'Office Diagnostic Testing', 'Infusion', 'Anticoag visit', 'Home Care Visit',
      'Office Visit Note', 'Cardiology Services', 'House Call Visit Note', 'Follow-Up')

    group by patient.patientId, unnest_providers.FirstName,
      unnest_providers.LastName, encounter.Type.Name, encounter.DateTime.instant,
      unnest_locations.Type.Name, unnest_providers.Role.Name, encounter.DateTime.date

)

select * from final
