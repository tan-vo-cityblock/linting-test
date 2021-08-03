

{{
  config(
    materialized='ephemeral'
  )
}}

with source as (

    select * from `cbh-lesli-ott.cost_use_apr_19.eh_all_claims_apr_19_f`

),

base_event_data as (

    select
        CLAIM_ID,
        patientId,
        ATT_PROV,
        BILL_PROV,
        PROV_LNAME,
        p.provider.PROV_NPI,
        ICD_DIAG_01,
        DIS_STAT,
        dis_stat_desc,
        ADM_DATE,
        DIS_DATE,
        PROC_CODE,
        bucket,
        sub_bucket,
        acute_via_ed,

        [
            STRUCT('admits' AS visitPart, FROM_DATE AS occuredAt),
            STRUCT('discharges' AS visitPart, TO_DATE AS occuredAt)
        ] as event_data

    from source as f

    left join `emblem-data.silver_claims.providers` p
      on f.ATT_PROV = p.provider.PROV_ID and
      f.SERVICINGPROVLOCSUFFIX = p.provider.PROV_LOC

    where patientId is not null and
      bucket IN ('ED', 'Acute Inpatient')

),

split_lines as (

    select
        CLAIM_ID,
        patientId,
        ATT_PROV,
        BILL_PROV,
        PROV_LNAME,
        ICD_DIAG_01,
        DIS_STAT,
        dis_stat_desc,
        ADM_DATE,
        DIS_DATE,
        PROC_CODE,
        PROV_NPI,
        bucket,
        sub_bucket,
        acute_via_ed,
        event_data.*

    from base_event_data

    cross join UNNEST(base_event_data.event_data) AS event_data

),

final as (

    select

        STRUCT(
             ARRAY_AGG(distinct CLAIM_ID) as ids,
            'CLAIM_ID' as idField,
            'cbh-lesli-ott' as project,
            'cost_use_apr_19' as dataset,
            'eh_all_claims_apr_19_f' as table
          ) as source,

        STRUCT(
           'facility' as `type`,
           case when BILL_PROV = '' then null else BILL_PROV end as `key`,
           PROV_LNAME as `display`
        ) as subject,

        visitPart as verb,

        STRUCT(
          'patientId' as `type`,
          patientId as `key`,
          STRING(NULL) as `display`
        ) as directObject,

        STRUCT(
          'setting' as `type`,
          case when bucket = 'ED' then 'ED'
               when REGEXP_CONTAINS(bucket, 'Inpatient') then 'Inpatient'
               else bucket end as `key`
        ) as indirectObject,

        STRUCT(
          'market' as `type`,
          'New York City' as `key`
        ) as prepositionalObject,

        STRUCT(
          cast(occuredAt as TIMESTAMP) as createdAt,
          TIMESTAMP(NULL) as completedAt,
          TIMESTAMP(NULL) as manualEventAt,
          TIMESTAMP(NULL) as receivedAt
        ) as timestamp,

        [
          STRUCT(
            'medicalService' as `type`,
            'acute' as `key`
          )
        ] as purposes,

        case
          when visitPart = 'discharges' then
          [
            STRUCT(
              'dischargeToSetting' as `type`,
               case when DIS_STAT <> '' then dis_stat_desc else null end as `key`
            )
          ]

      --     when visitpart = 'admits' and sum(acute_via_ed) > 0 then
      --       STRUCT(
      --         'transferFromSetting' as `type`,
      --         'ED' as `key`
      --       )
      --     else null

        end as outcomes

    from split_lines

    group by
      patientId,
      occuredAt,
      visitPart,
      BILL_PROV,
      PROV_LNAME,
      DIS_STAT,
      dis_stat_desc,
      bucket,
      sub_bucket

)

select * from final
