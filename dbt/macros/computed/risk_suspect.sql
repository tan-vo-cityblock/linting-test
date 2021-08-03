
{% macro risk_suspect(slug,
                      min_female_age=None,
                      min_male_age=None,
                      max_age=None,
                      evidence_slugs=None,
                      lab_level_slug=None,
                      lab_level_negative_result="stable",
                      screening_tool_slug=None,
                      min_screening_tool_score=None,
                      vitals_code=None,
                      min_vitals_value=None,
                      period="1 year",
                      condition_slugs=None,
                      condition_negative_result="false",
                      hccs=None,
                      age_table=ref('member_info')
                     )%}

{% set code_like_clause=generate_code_like_clause(
      hccs=hccs) %}

{%- set evidence_cte_list = [] %}
{%- set diagnoses_cte_list = [] %}

with 

{% if min_female_age and min_male_age and max_age %}

age_eligible_members as (

  {{ find_age_eligible_members(age_table, min_female_age, min_male_age, max_age) }}

),

{% endif %}

{% if evidence_slugs %}
{% set _ = evidence_cte_list.append('members_with_condition_field_evidence') %}

  {{ find_members_w_field_value(evidence_slugs, final_cte_name='members_with_condition_field_evidence') }}

{% endif %}

{% if lab_level_slug and lab_level_negative_result %}
{% set _ = evidence_cte_list.append('members_with_lab_field_evidence') %}

  {{ find_members_w_field_value([lab_level_slug], fieldValueOp="!=", fieldValue=lab_level_negative_result, final_cte_name="members_with_lab_field_evidence") }}

{% endif %}

{% if screening_tool_slug and min_screening_tool_score %}
{% set _ = evidence_cte_list.append('members_with_screening_tool_evidence') %}

members_with_screening_tool_evidence as (

  select distinct patientId
  from {{ source('commons', 'patient_screening_tool_submission')}} psts
  inner join {{ source('commons', 'builder_screening_tool') }} st
  on 
  	psts.screeningToolSlug = st.slug and
  	st.slug = '{{ screening_tool_slug }}'
  where 
  	psts.score >= {{ min_screening_tool_score }} and 
  	psts.deletedAt is null

),

{% endif %}

{% if vitals_code and min_vitals_value and period %}
{% set _ = evidence_cte_list.append('members_with_vitals_evidence') %}

members_with_vitals_evidence as (

  select patientId
  from {{ ref('vitals') }}
  where
    code = '{{vitals_code}}' and
    date(timestamp) >= date_sub(current_date, interval {{ period }})
  group by patientId
  having max(value) >= {{ min_vitals_value }}

),

{% endif %}

age_eligible_members_with_any_evidence as (

  select patientId
  from (

    {{ union_cte_list(evidence_cte_list) }}

  )

  {% if min_age and max_age %}

  inner join age_eligible_members
  using (patientId)

  {% endif %}

),

{% if condition_slugs %}
{% set _ = diagnoses_cte_list.append('members_with_condition_field_diagnosis') %}

  {{ find_members_w_field_value(condition_slugs, fieldValueOp="!=", fieldValue=condition_negative_result, final_cte_name="members_with_condition_field_diagnosis") }}

{% endif %}

{% if hccs %}
{% set _ = diagnoses_cte_list.append('members_with_claims_or_ccd_diagnosis') %}

codes as (

  select diagnosis_code as diagnosisCode
  from {{ source('codesets', 'hcc_2020') }}
  where {{ code_like_clause }}),

members_with_claims_or_ccd_diagnosis as (

  select distinct memberIdentifier as patientId
  from {{ ref('abs_diagnoses') }}
  inner join codes
  using (diagnosisCode)
  where 
    memberIdentifierField = 'patientId' and
    serviceDateFrom >= date_sub(date_trunc(current_date, year), interval {{ period }}) and

    (
      sourceType = 'claim' or 
      source = 'ccd'
    )

),

{% endif %}

members_with_any_diagnosis as (

  {{ union_cte_list(diagnoses_cte_list) }}

),

cf_status as (

  select 
    patientId,
    case
      when patientId in (select * from members_with_any_diagnosis)
        then 'false'
      else 'true'
    end as value
  from age_eligible_members_with_any_evidence
  group by patientId

),

final as (

  {{ computed_field(slug=slug, type='risk_suspect', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
