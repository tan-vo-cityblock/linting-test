
{{ config(tags=['nightly', 'payer_list']) }}

{%- set payer_list = var('payer_list') -%}
{%- set cte_list = [] -%}

with

{% for payer in payer_list %}

  {% if payer in ['tufts', 'cardinal', 'healthy_blue'] %}

    {# do nothing; we do not receive lab results from these partners #}

  {% else %}

{% set _ = cte_list.append(payer) %}

{{ payer }} as (

  select distinct
    coalesce(memberIdentifier.patientId, memberIdentifier.commonId, memberIdentifier.partnerMemberId) as memberIdentifier,

    case
      when memberIdentifier.patientId is not null
        then 'patientId'
      when memberIdentifier.commonId is not null
        then 'commonId'
      else 'partnerMemberId'
    end as memberIdentifierField,

    name,
    resultNumeric,
    units,
    loinc,
    date,
    'claims' as sourceType,
    '{{ payer }}' as sourcePartner

  from {{ source(payer, 'LabResult') }}

),

  {% endif %}

{% endfor %}

{% set _ = cte_list.append('medical_orders') %}

medical_orders as (

  select distinct
    po.patient.patientId as memberIdentifier,
    'patientId' as memberIdentifierField,
    results.Description as name,
    SAFE_CAST(REGEXP_REPLACE(results.Value, r"[<>^%]", "") as numeric) as resultNumeric,
    case
      when trim(results.Units) = '' then null
      else trim(results.Units)
    end as units,
    regexp_replace(results.Code, r"[-]", "") as loinc,
    DATE(po.order.CompletionDateTime.instant) as date,
    'ehr-observation-results' as sourceType,
    lower(patient.source.name) as sourcePartner
  from
    {{ source('medical', 'patient_orders') }} po,
    UNNEST(po.order.Results) as results
  where patient.patientId is not null

),

{% set _ = cte_list.append('medical_results') %}

medical_results as (

  select distinct
    patient.patientId as memberIdentifier,
    'patientId' as memberIdentifierField,
    observations.Name as name,
    safe_cast(regexp_replace(observations.Value, r"[<>^%]", "") as numeric) as resultNumeric,
    case
      when trim(observations.Units) = '' then null
      else trim(observations.Units)
    end as units,
    regexp_replace(observations.Code, r"[-]", "") as loinc,
    date(observations.DateTime.instant) as date,
    'ehr-point-of-care' as sourceType,
    patient.source.name as sourcePartner
  from {{ source('medical', 'patient_results') }},
  unnest(result.Observations) as observations
  where observations.CodeSystemName = 'LOINC'

),

loinc_names as (

  select
    loincNumberAltId2 as loinc,
    longCommonName as loincName
    
  from {{ ref('src_loinc_table_core') }}

),

combined_results as (

  {% for cte in cte_list %}

  select * from {{ cte }}

    {% if not loop.last %}

    union all

    {% endif %}

  {% endfor %}

),

final as (

  select
    cr.* except(date, sourceType, sourcePartner),
    ln.loincName,
    cr.date,
    cr.sourceType,
    cr.sourcePartner
    
  from combined_results cr
  
  left join loinc_names ln
  using (loinc)

)

select * from final
