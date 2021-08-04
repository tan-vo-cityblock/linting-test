
{{
  config(
    materialized='table'
  )
}}

with base as (

    select
      base.claimId,
      base.lineId,
      base.providerServicingSpecialty,
      IFNULL(cat.providerCategory, 'unmapped') as providerCategory

    from {{ ref('abs_professional_flat') }} as base

    left join {{ source('claims_mappings', 'provider_code_to_category') }} as cat
      on base.providerServicingSpecialty = cat.code

),

flags as (

    select
        claimId,
        lineId,
        providerCategory,

        case
          when providerCategory = 'primaryCare' then true
          else false
          end
        as primaryCareProviderFlag,

        case
          when providerCategory = 'surgical' then true
          else false
          end
        as surgicalProviderFlag,

        case
          when providerCategory = 'specialist' then true
          else false
          end
        as specialistProviderFlag,

        case
          when providerCategory = 'behavioralHealth' then true
          else false
          end
        as behavioralHealthProviderFlag,

        case
          when providerServicingSpecialty = '18' then true
          else false
          end
        as ophthalmologyProviderFlag

    from base

)

select * from flags
