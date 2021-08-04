
{% macro lab_level(slug,
          lab_codes,
          mild_min,
          moderate_min,
          severe_min,
          critical_min=None,
          stable_cpt=None,
          mild_cpt=None,
          moderate_cpt=None,
          period="1 YEAR",
          table=ref('lab_results'),
          cpt_table=ref('abs_procedures')) %}


with loinc_results as (

  SELECT
    memberIdentifier as patientId,
    date,
    max(resultNumeric) AS result
  from {{ table }}
  where 
    memberIdentifierField = 'patientId' and
    ( {{ chain_or("loinc", "like", lab_codes) }} ) and 
    DATETIME(date) > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ period }}) and 
    resultNumeric > 0
  group by memberIdentifier, date

),

{% if stable_cpt and mild_cpt and moderate_cpt %}
derived_results as (

  select
    memberIdentifier as patientId, 
    serviceDateFrom as date, 
    case
      when max(procedureCode) = '{{ stable_cpt }}' then ( {{ mild_min }} - 0.1 )
      when max(procedureCode) = '{{ mild_cpt }}' then {{ mild_min }}
      when max(procedureCode) = '{{ moderate_cpt }}' then ( {{ severe_min }} - 0.1 )
    end as result
  from {{ cpt_table }}
  where 
    memberIdentifierField = 'patientId' and
    procedureCode in ('{{ stable_cpt }}','{{ mild_cpt }}','{{ moderate_cpt }}') and
    serviceDateFrom > date_sub(current_date, interval {{ period }})
  group by memberIdentifier, serviceDateFrom

),
{% endif %}

all_results as (

  select patientId, date, result from loinc_results
  {% if stable_cpt and mild_cpt and moderate_cpt %}
  union distinct
  select patientId, date, result from derived_results
  {% endif %}
  
),

max_results as (

  select patientId, date, max(result) as result
  from all_results
  group by patientId, date
  
),

recent_results as (

  select most_recent.* except (date) from (
  
    select array_agg(max_results order by date desc limit 1)[offset(0)] most_recent
    from max_results 
    group by patientId
  
  )
),

cf_status as (

  select
    patientId,

    CASE
      WHEN result < {{ mild_min }} then "stable"
      WHEN result >= {{ mild_min }} and result < {{ moderate_min }} then "mild"
      WHEN result >= {{ moderate_min }} and result < {{ severe_min }} then "moderate"

      {% if critical_min %}
      WHEN result >= {{ severe_min }} and result < {{ critical_min }} then "severe"
      {% else %}
      WHEN result >= {{ severe_min }} then "severe"
      {% endif %}

      {% if critical_min %}
      WHEN result >= {{ critical_min }} then "critical"
      {% endif %}

    END as value

  from recent_results
  GROUP BY patientId, result

),

final as (

    {{ computed_field(slug=slug, type='lab_level', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
