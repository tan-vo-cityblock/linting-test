
{{
  config(
    materialized='incremental'
  )
}}

with results as (

  select
    lr.patientId,
    'ICD10' as codeType,
    ccm.codeValue,
    ccm.hccs,
    case 
      when lr.fieldValue = 'true' then 'OPEN'
      when lr.fieldValue = 'false' then 'CLOSED'
    end as opportunityStatus,
    current_timestamp as createdAt
  from {{ ref('latest_computed_field_results') }} lr
  inner join {{ source('computed_candidates', 'computed_candidate_mapping') }} ccm
  using (fieldSlug)

),

final as (

  select
    r.patientId,
    r.codeType,
    r.codeValue,
    d.icd_10_cm_code_description as codeDescription,
    r.hccs,
    r.opportunityStatus,
    r.createdAt
  from results r
  inner join {{ source('codesets', 'ccs_dx_icd10cm') }} d
  on regexp_replace(r.codeValue, r"[.]", "") = d.icd_10_cm_code
  order by r.patientId, r.codeValue

)

select * from final