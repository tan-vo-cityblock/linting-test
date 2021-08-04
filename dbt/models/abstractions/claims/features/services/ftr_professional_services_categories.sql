
{{
  config(
    materialized='table'
  )
}}

with base as (

    select
      base.claimId,
      base.lineId,
      cat.hcciCategory as serviceCategory,
      cat.hcciDetailedServiceCategory as serviceSubCategory

    from {{ ref('abs_professional_flat') }} as base

    left join {{ source('ref_hcci', 'ref_hcci_section_44') }} as cat
      on base.procedureCode = cat.cpt

),

flags as (

    select
        *

    from base

)

select * from flags
