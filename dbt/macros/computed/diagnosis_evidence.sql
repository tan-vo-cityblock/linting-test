{%- macro diagnosis_evidence(slug,
      icd_diagnoses=None,
      excluded_icd_diagnoses=None,
      procedure_codes=None,
      excluded_snomed_codes=None,
      value_set_name=None,
      period="1 year",
      period_op= ">",
      calendar_year=False,
      any_encounter=False,
      min_any_encounter_claims=1,
      min_inpatient_claims=1,
      min_acute_facility_claims=1,
      min_other_claims=2
    ) -%}

{%- if icd_diagnoses -%}

  {%- set diagnosis_like_clause=chain_or('code', 'like', icd_diagnoses) -%}
  {%- set problem_like_clause=chain_or('mapTarget', 'like', icd_diagnoses) -%}

{%- endif -%}

{%- if procedure_codes -%}

{%- set procedure_like_clause=chain_or('code', 'like', procedure_codes) -%}

{%- endif -%}

with

{% if icd_diagnoses or value_set_name %}

abs_diagnoses as (

  select
  distinct memberIdentifier as patientId,
  sourceId,
  id,
  'id' as key,
  'abs_diagnoses' as model,
  diagnosisCode as code,
  serviceDateFrom as validDate,
  encounterType

  from {{ ref('abs_diagnoses') }}
  where memberIdentifierField = 'patientId'

  {% if calendar_year == True %}
  and date_trunc(serviceDateFrom, year) {{ period_op }} date_sub(date_trunc(current_date, year), interval 1 year)

  {% else %}
  and serviceDateFrom {{ period_op }} date_sub(current_date, interval {{ period }})
  {%- endif -%}

),

{%- endif -%}

{% if procedure_codes %}

abs_procedures as (

  select
  distinct memberIdentifier as patientId,
  sourceId,
  id,
  'id' as key,
  'abs_procedures' as model,
  procedureCode as code,
  serviceDateFrom as validDate,
  encounterType

  from {{ ref('abs_procedures') }}
  where memberIdentifierField = 'patientId'

  {% if calendar_year == True %}
  and date_trunc(serviceDateFrom, year) {{ period_op }} date_sub(date_trunc(current_date, year), interval 1 year)

  {% else %}
  and serviceDateFrom {{ period_op }} date_sub(current_date, interval {{ period }})
  {%- endif -%}

),

{%- endif -%}

{%- if value_set_name -%}

hedis_value_set_codes as (

  select
  regexp_replace(code, r'[.]', '') as code

  from {{ source('hedis_codesets', 'vsd_value_set_to_code') }}
  where value_set_name = '{{ value_set_name }}'

),

{% elif icd_diagnoses %}

snomed_to_icd10_codes as (

  select
  snomedCode as code,
  mapTarget

  from {{ ref('src_reference_snomed_to_icd10') }}
  where ({{ problem_like_clause }})

  {%- if excluded_snomed_codes -%}

  and snomedCode not in {{ list_to_sql(excluded_snomed_codes) }}

  {%- endif -%}

),

problem_list_diagnoses as (

  select
  patientId

  from abs_diagnoses
  inner join snomed_to_icd10_codes s
  using (code)

  ),

{%- endif -%}

{% if icd_diagnoses or value_set_name %}

billed_diagnoses as (

  select *

  from abs_diagnoses

  {% if value_set_name %}

  inner join hedis_value_set_codes
  using (code)

  {% elif icd_diagnoses %}

  where ({{ diagnosis_like_clause }})

  {%- endif -%}

  {% if excluded_icd_diagnoses %}

  and code not in {{ list_to_sql(excluded_icd_diagnoses) }}

  {% endif %}

),

{% endif %}

{% if procedure_codes %}

billed_procedures as (

  select *

  from abs_procedures
  where ({{ procedure_like_clause }})
  and encounterType in ('inpatient', 'other')

),

{% endif %}

billed_encounters as (

{%- if icd_diagnoses or value_set_name -%}

  select *
  from billed_diagnoses

{% endif %}

{%- if icd_diagnoses and procedure_codes -%}

  union all

{% endif %}

{%- if procedure_codes -%}

  select *
  from billed_procedures

{% endif %}

),

inpatient_encounters as (

  select patientId
  from billed_encounters
  where encounterType = 'inpatient'
  group by patientId
  having count(distinct sourceId) >= {{ min_inpatient_claims }}

),

outpatient_encounters as (

  select patientId
  from billed_encounters
  where encounterType in ('other', 'ed')
  group by patientId
  having count(distinct sourceId) >= {{ min_other_claims }}

),

acute_facility_encounters as (

  select patientId
  from billed_encounters
  where encounterType in ('inpatient', 'ed')
  group by patientId
  having count(distinct sourceId) >= {{ min_acute_facility_claims }}

),

{% if any_encounter == True %}

any_encounters as (

  select patientId
  from billed_encounters
  group by patientId
  having count(distinct sourceId) >= {{ min_any_encounter_claims }}

),

{%- endif -%}

combined_encounters as (

  select * from inpatient_encounters
  union distinct
  select * from outpatient_encounters
  union distinct
  select * from acute_facility_encounters

  {% if any_encounter %}

  union distinct
  select * from any_encounters

  {%- endif -%}

  {% if icd_diagnoses %}

  union distinct
  select * from problem_list_diagnoses

  {%- endif -%}

),

evidence as (

  select a.patientId, b.id, b.key, b.model, b.code, b.validDate

  from combined_encounters a
  left join billed_encounters b
  using (patientId)

),

aggregated_resources as (

  {{ aggregate_computed_field_resources(table='evidence') }}

),

cf_status as (

  {{ derive_computed_field_values(table='aggregated_resources') }}

),

final as (

  {{ computed_field(slug=slug, type='diagnosis_evidence', value='value', table='cf_status', evaluated_resource='evaluatedResource') }}

)

select *
from final

{%- endmacro -%}
