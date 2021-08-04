
{{
  config(
    materialized='table'
  )
}}

with base as (

    select
      claimId,
      lineId,
      procedureCode

    from {{ ref('abs_professional_flat') }}

),

flags as (

    select
        claimId,
        lineId,

        case
          when countif(procedureCode in (
            'A0427','A0429','A0433', 'A0434',
            'A0430', 'A0431', 'A0225'
          )) over (PARTITION BY claimId) > 0 then true
          else false
          end
        as emergentAmbulanceServiceFlag

    from base

)

select * from flags
