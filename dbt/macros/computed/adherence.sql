

{% macro adherence(slug,
           procedure_codes,
           condition_slugs,
           min_result_threshold='(146.0 / 12.0)',
           measurement_period='MONTH',
           period_begin="15 MONTH",
           period_end="3 MONTH",
           table=ref('abs_procedures')) %}

with 

{{ find_members_w_field_value(condition_slugs) }}

services as (

    select distinct
        memberIdentifier as patientId,
        sourceId,
        procedureCode,
        date_trunc(serviceDateFrom, {{ measurement_period }}) AS measurement_period

    from {{ table }}

    where
      memberIdentifierField = 'patientId' and
      DATETIME(serviceDateFrom) > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ period_begin }}) and 
      DATETIME(serviceDateFrom) < DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ period_end }}) and 
      procedureCode in {{ list_to_sql(procedure_codes) }} and 
      memberIdentifier in (select * from members_w_included_conditions)

),

count_per_period as (

    select
        patientId,
        measurement_period,
        count(measurement_period) AS count

    from services

    group by patientId, measurement_period

),

positive as (

    select
        patientId

    from count_per_period

    group by patientId

    having (sum(count) / count(distinct measurement_period)) < {{ min_result_threshold }}

),

cf_status AS (

    select
        memberIdentifier as patientId,

        CASE
          WHEN memberIdentifier IN (SELECT * FROM positive) THEN "true"
          ELSE "false"
          END
        AS value

    from {{ table }}

    where memberIdentifierField = 'patientId'

    group by memberIdentifier

),

final as (

    {{ computed_field(slug=slug, type='adherence', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
