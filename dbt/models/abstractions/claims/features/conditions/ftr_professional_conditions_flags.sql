
{{
  config(
    materialized='table'
  )
}}

with base as (

    select distinct
      claimId,
      lineId,
      principalDiagnosisCode

    from {{ ref('abs_professional_flat') }}

),

flags as (

    select
        claimId,
        lineId,

        case
          when (
            {{ chain_or("principalDiagnosisCode", "like",
              ['O', 'P', 'Q', 'Z34', 'Z38',
              'Z37', 'Z3A', 'Z302', 'Z412', 'Z0011']) }}
          ) then true
          else false
          end
        as maternityConditionFlag,

        case
          when principalDiagnosisCode like 'F%' then true
          else false
          end
        as psychConditionFlag

    from base

)

select * from flags
