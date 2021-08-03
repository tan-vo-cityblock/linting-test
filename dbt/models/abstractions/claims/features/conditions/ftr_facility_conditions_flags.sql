
{{
  config(
    materialized='table'
  )
}}

with base as (

    select
      claimId,
      principalDiagnosisCode

    from {{ ref('abs_facility_flat') }}

),

flags as (

    select distinct

        claimId,

        case
          when principalDiagnosisCode like 'F1%' then true
          else false
          end
        as substanceAbuseConditionFlag,

        case
          when (
            principalDiagnosisCode like 'F0%' or
            principalDiagnosisCode like 'F2%' or
            principalDiagnosisCode like 'F3%' or
            principalDiagnosisCode like 'F4%' or
            principalDiagnosisCode like 'F5%' or
            principalDiagnosisCode like 'F6%' or
            principalDiagnosisCode like 'F7%' or
            principalDiagnosisCode like 'F8%' or
            principalDiagnosisCode like 'F9%'
          ) then true
          else false
          end
        as psychConditionFlag

    from base

)

select * from flags
