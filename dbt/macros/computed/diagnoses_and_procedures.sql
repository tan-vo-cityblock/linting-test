
{% macro diagnoses_and_procedures(slug,
             eligible_gender=None,
             condition_slugs=None,
             all_conditions=False,
             icd_diagnoses=None,
             excluded_icd_diagnoses=None,
             snomed_codes=None,
             excluded_snomed_codes=None,
             procedure_codes=None,
             drg_codes=None,
             drg_emergent=False,
             hccs=None,
             provider_specialty_codes=None,
             value_set_name=None,
             value_set_join_column="diagnosisCode",
             medication_slug=None,
             lab_result_slug=None,
             period="1 YEAR",
             period_op=">",
             calendar_year=False,
             min_inpatient_claims=1,
             min_acute_facility_claims=1,
             min_other_claims=2,
             use_problems=True,
             use_any=False,
             use_hie=False,
             min_unique_days=None,
             principal_dx_only=False,
             gender_table=ref('member_info'),
             diag_table=ref('abs_diagnoses'),
             cpt_table=ref('abs_procedures'),
             drg_table=ref('abs_drg'),
             member_table=var('cf_member_table')

           )%}

{% set code_like_clause=generate_code_like_clause(
      icd_diagnoses=icd_diagnoses,
      procedure_codes=procedure_codes) %}

{% set hccs_like_clause=generate_code_like_clause(
      hccs=hccs) %}

{% if use_problems == True and icd_diagnoses %}

  {% set problems_like_clause=generate_code_like_clause(
        icd_diagnoses=icd_diagnoses,
        use_problems=use_problems) %}

{% endif %}

{% if provider_specialty_codes %}

  {% set provider_specialty_codes_like_clause=generate_code_like_clause(
          provider_specialty_codes=provider_specialty_codes) %}

{% endif %}

{%- set cte_list = [] %}

with

{% if eligible_gender %}

gender_eligible_members as (

  select patientId
  from {{ gender_table }}
  where gender = '{{ eligible_gender }}'

),

{% endif %}

{% if condition_slugs %}
{% set _ = cte_list.append('members_w_included_conditions') %}

  {{ find_members_w_field_value(condition_slugs, all_slugs=all_conditions) }}

{% endif %}

{% if value_set_name %}

codes as (

  select regexp_replace(code, r"[.]", "") as code
  from {{ source('hedis_codesets', 'vsd_value_set_to_code') }}
  where value_set_name = '{{ value_set_name }}'

),

{% endif %}

{% if hccs %}

hcc_codes as (

  select diagnosis_code as diagnosisCode
  from {{ source('codesets', 'hcc_2020') }}
  where {{ hccs_like_clause }}

),

{% endif %}

{% if icd_diagnoses or hccs or procedure_codes or value_set_name %}
{% set _ = cte_list.append('inpatient_claims') %}
inpatient_claims as (

  select memberIdentifier as patientId

  {% if value_set_join_column == 'procedureCode' %}

  from {{ cpt_table }} a

    {% if icd_diagnoses %}

      left join {{ diag_table }}
      using (memberIdentifier, sourceId, encounterType, serviceDateFrom, providerSpecialty)

    {% endif %}

  {% else %}

  from {{ diag_table }} a

  {% endif %}

  {% if value_set_name %}

  inner join codes
  on {{value_set_join_column}} = codes.code

  {% endif %}

  {% if hccs %}

  inner join hcc_codes
  using (diagnosisCode)

  {% endif %}

  {% if procedure_codes %}

    left join {{ cpt_table }}
    using (memberIdentifier, sourceId, encounterType, serviceDateFrom, providerSpecialty)

  {% endif %}

  where

    a.memberIdentifierField = 'patientId' and

    {% if icd_diagnoses or procedure_codes %}

      ( {{ code_like_clause }} ) and

    {% endif %}

      encounterType = 'inpatient' and

    {% if principal_dx_only %}
       diagnosisTier = 'principal' and

    {% endif %}

    {% if excluded_icd_diagnoses %}

    diagnosisCode NOT IN {{ list_to_sql(excluded_icd_diagnoses) }} AND

    {% endif %}

    {% if calendar_year == True %}

      date_trunc(serviceDateFrom, year) {{ period_op }} date_sub(date_trunc(current_date, year), interval {{ period }})

    {% else %}

      DATETIME(serviceDateFrom) {{ period_op }} DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ period }})

    {% endif %}

    {% if provider_specialty_codes %}

    and ( {{ provider_specialty_codes_like_clause}} )

    {% endif %}

  group by memberIdentifier
  having COUNT(DISTINCT sourceId) >=  {{ min_inpatient_claims }}
    {% if min_unique_days %}
    and COUNT(DISTINCT serviceDateFrom) >= {{ min_unique_days }}
    {% endif %}

),
{% endif %}

{% if icd_diagnoses or hccs or procedure_codes or value_set_name %}
{% set _ = cte_list.append('other_claims') %}
other_claims as (

    select memberIdentifier as patientId

    {% if value_set_join_column == 'procedureCode' %}

    from {{ cpt_table }} a

      {% if icd_diagnoses %}

        left join {{ diag_table }}
        using (memberIdentifier, sourceId, encounterType, serviceDateFrom, providerSpecialty)

      {% endif %}

    {% else %}

    from {{ diag_table }} a

    {% endif %}

    {% if value_set_name %}

      inner join codes
      on {{ value_set_join_column }} = codes.code

    {% endif %}

  {% if hccs %}

    inner join hcc_codes
    using (diagnosisCode)

  {% endif %}

  {% if procedure_codes %}

    left join {{ cpt_table }}
    using (memberIdentifier, sourceId, encounterType, serviceDateFrom, providerSpecialty)

  {% endif %}

    where

      a.memberIdentifierField = 'patientId' and

      {% if icd_diagnoses or procedure_codes %}

        ( {{ code_like_clause }} ) and

      {% endif %}

        encounterType in ('other','ed') and

      {% if principal_dx_only %}
        diagnosisTier = 'principal' and

      {% endif %}

      {% if excluded_icd_diagnoses %}

      diagnosisCode NOT IN {{ list_to_sql(excluded_icd_diagnoses) }} AND

      {% endif %}

      {% if calendar_year == True %}

        date_trunc(serviceDateFrom, year) {{ period_op }} date_sub(date_trunc(current_date, year), interval {{ period }})

      {% else %}

        DATETIME(serviceDateFrom) {{ period_op }} DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ period }})

      {% endif %}

      {% if provider_specialty_codes %}

      and ( {{ provider_specialty_codes_like_clause}} )

      {% endif %}

    group by memberIdentifier
    having COUNT(DISTINCT sourceId) >= {{ min_other_claims }}
      {% if min_unique_days %}
      and COUNT(DISTINCT serviceDateFrom) >= {{ min_unique_days }}
      {% endif %}

),
{% endif %}

{% if icd_diagnoses or procedure_codes or value_set_name %}
{% set _ = cte_list.append('acute_facility_claims') %}
acute_facility_claims as (

    select memberIdentifier as patientId

    {% if value_set_join_column == 'procedureCode' %}

    from {{ cpt_table }} a

      {% if icd_diagnoses %}

        left join {{ diag_table }}
        using (memberIdentifier, sourceId, encounterType, serviceDateFrom, providerSpecialty)

      {% endif %}

    {% else %}

    from {{ diag_table }} a

    {% endif %}

    {% if value_set_name %}

      inner join codes
      on {{ value_set_join_column }} = codes.code

    {% endif %}

  {% if procedure_codes %}

    left join {{ cpt_table }}
    using (memberIdentifier, sourceId, encounterType, serviceDateFrom, providerSpecialty)

  {% endif %}

    where

      a.memberIdentifierField = 'patientId' and

      {% if icd_diagnoses or procedure_codes %}

        ( {{ code_like_clause }} ) and

      {% endif %}

        encounterType in ('inpatient','ed') and

      {% if principal_dx_only %}
        diagnosisTier = 'principal' and

      {% endif %}

      {% if excluded_icd_diagnoses %}

      diagnosisCode NOT IN {{ list_to_sql(excluded_icd_diagnoses) }} AND

      {% endif %}

      {% if calendar_year == True %}

        date_trunc(serviceDateFrom, year) {{ period_op }} date_sub(date_trunc(current_date, year), interval {{ period }})

      {% else %}

        DATETIME(serviceDateFrom) {{ period_op }} DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ period }})

      {% endif %}

      {% if provider_specialty_codes %}

      and ( {{ provider_specialty_codes_like_clause}} )

      {% endif %}

    group by memberIdentifier
    having COUNT(DISTINCT sourceId) >= {{ min_acute_facility_claims }}
      {% if min_unique_days %}
      and COUNT(DISTINCT serviceDateFrom) >= {{ min_unique_days }}
      {% endif %}

),
{% endif %}




{% if use_problems == True and icd_diagnoses %}
{% set _ = cte_list.append('problems') %}

problem_ref as (

    SELECT
      cast(referencedComponentId as string) AS snomedCode,
      REGEXP_REPLACE(mapTarget, r"[.]", "") AS mapTarget
    FROM {{ source('code_maps', 'snomed_to_icd10') }}

),

problems AS (

    select memberIdentifier as patientId
    from {{ diag_table }} as diag
    INNER JOIN problem_ref as pr
    ON pr.snomedCode = diag.diagnosisCode
    WHERE
      memberIdentifierField = 'patientId' and

      ( {{ problems_like_clause }} ) and

      {% if excluded_snomed_codes %}

      snomedCode not in {{ list_to_sql(excluded_snomed_codes) }} and

      {% endif %}

      {% if excluded_icd_diagnoses %}

      mapTarget NOT IN {{ list_to_sql(excluded_icd_diagnoses) }} AND

      {% endif %}

      diag.diagnosisCodeset = "snomedct" AND
      DATETIME(diag.serviceDateFrom) > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ period }})
    group by memberIdentifier

),

{% endif %}

{% if use_hie == True %}
{% set _ = cte_list.append('hie_disch_diag') %}

hie_disch_diag as (

    select memberIdentifier as patientId
    from {{ diag_table }}
    WHERE
      memberIdentifierField = 'patientId' and
      ( {{ code_like_clause }} ) AND
      encounterType = 'discharge' AND
      DATETIME(serviceDateFrom) > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ period }})
    group by memberIdentifier

),

{% endif %}

{% if drg_codes %}
{% set _ = cte_list.append('drgs') %}

drgs as (

    select memberIdentifier as patientId
    from {{ drg_table }}
    WHERE
      drgCode in {{ list_to_sql(drg_codes) }} AND

      {% if drg_emergent == True %}

        edServiceFlag and

      {% endif %}

      DATETIME(serviceDateFrom) > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ period }})
    group by memberIdentifier

),

{% endif %}

{% if use_any == True %}
{% set _ = cte_list.append('any_diag') %}

any_diag as (

  select memberIdentifier as patientId

  {% if value_set_join_column == 'procedureCode' %}

  from {{ cpt_table }} a

    {% if icd_diagnoses %}

      left join {{ diag_table }}
      using (memberIdentifier, sourceId, serviceDateFrom, providerSpecialty)

    {% endif %}

  {% else %}

  from {{ diag_table }} a

  {% endif %}

  {% if value_set_name %}

  inner join codes
  on {{ value_set_join_column }} = codes.code

  {% endif %}

  {% if procedure_codes %}

    left join {{ cpt_table }}
    using (memberIdentifier, sourceId, serviceDateFrom, providerSpecialty)

  {% endif %}

  where

    a.memberIdentifierField = 'patientId' and

    {% if icd_diagnoses or procedure_codes %}

      ( {{ code_like_clause }} ) and

    {% endif %}

    {% if snomed_codes %}

      diagnosisCode in {{ list_to_sql(snomed_codes) }} and

    {% endif %}

    {% if calendar_year == True %}

      date_trunc(serviceDateFrom, year) {{ period_op }} date_sub(date_trunc(current_date, year), interval {{ period }})

    {% else %}

      DATETIME(serviceDateFrom) {{ period_op }} DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ period }})

    {% endif %}

    {% if provider_specialty_codes %}

    and ( {{ provider_specialty_codes_like_clause}} )

    {% endif %}

  group by memberIdentifier

  {% if min_unique_days %}

    having count(distinct serviceDateFrom) >= {{ min_unique_days }}

  {% endif %}

),

{% endif %}

{% if medication_slug %}
{% set _ = cte_list.append('medications') %}

  {{ find_members_w_field_value([medication_slug], final_cte_name='medications') }}

{% endif %}

{% if lab_result_slug %}
{% set _ = cte_list.append('lab_results') %}

  {{ find_members_w_field_value([lab_result_slug], final_cte_name='lab_results') }}

{% endif %}

members_w_evidence as (

  {{ union_cte_list(cte_list) }}

),

included_members as (

  select patientId
  from members_w_evidence
  {% if eligible_gender %}
  inner join gender_eligible_members
  using (patientId)
  {% endif %}

),

cf_status as (

  select
    patientId,

    CASE
      WHEN patientId IN (select * from included_members)
        THEN "true"
      ELSE "false"
    END AS value

  from {{ member_table }}

  group by patientId

  ),

final as (

    {{ computed_field(slug=slug, type='diagnoses_and_procedures', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
