

{{
  config(
    materialized='table'
  )
}}

with acs_reference as (

    select
        code,
        cast(ed_care_needed_not_preventable as FLOAT64) as ed_care_needed_not_preventable,
        cast(ed_care_needed_preventable as FLOAT64) as ed_care_needed_preventable,
        cast(psych as FLOAT64) as psych,
        cast(injury as FLOAT64) as injury,
        cast(drug as FLOAT64) as drug,
        cast(alcohol as FLOAT64) as alcohol,
        cast(emergent_treatable as FLOAT64) as emergent_treatable,
        cast(non_emergent as FLOAT64) as non_emergent,
        cast(unclassified as FLOAT64) as unclassified

    from {{ source('claims_mappings', 'code_to_nyu_asc_patched') }}

),

base as (

    select
        base.claimId,
        acs_codes.ed_care_needed_not_preventable as acsEdNeededNotPreventable,
        acs_codes.ed_care_needed_preventable as acsEdNeededPreventable,
        acs_codes.psych as acsEdPsych,
        acs_codes.injury as acsEdInjury,
        acs_codes.drug as acsEdDrug,
        acs_codes.alcohol as acsEdAlcohol,
        acs_codes.emergent_treatable as acsEdEmergentTreatable,
        acs_codes.non_emergent as acsEdNonEmergent,
        acs_codes.unclassified as acsEdUnclassified

    from {{ ref('abs_facility_flat') }} as base

    left join acs_reference as acs_codes
      on base.principalDiagnosisCode = acs_codes.code

    left join {{ ref('ftr_facility_costs_categories') }} as buckets
      on base.claimId = buckets.claimId

    where buckets.costSubCategory = 'ed'

),

flags as (

    select distinct
        *,

        case
          when
            (
              acsEdEmergentTreatable +
              acsEdNonEmergent +
              acsEdDrug +
              acsEdAlcohol +
              acsEdPsych
            ) > .5
            then true
          else false
          end
        as acsEdFlag

    from base

)

select * from flags
