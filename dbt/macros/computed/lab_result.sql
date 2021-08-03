
{% macro lab_result(slug,
         lab_codes,
         min_result_threshold=None,
         max_result_threshold=None,
         period="1 YEAR",
         table=ref('lab_results')) %}


with ranked_values as (

    SELECT
        memberIdentifier as patientId,
        date,
        max(resultNumeric) AS result,
        RANK() OVER(PARTITION BY memberIdentifier ORDER BY date DESC) as rank

    from {{ table }}

    where 
      memberIdentifierField = 'patientId' and
      ( {{ chain_or("loinc", "like", lab_codes) }} ) and 
      DATETIME(date) > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ period }}) and
      resultNumeric > 0

    group by memberIdentifier, date

),

cf_status as (

    select
        patientId,

        CASE
          {% if min_result_threshold and max_result_threshold %}
          WHEN result >= {{ min_result_threshold }} and result < {{ max_result_threshold }} THEN "true"
          {% elif min_result_threshold %}
          WHEN result >= {{ min_result_threshold }} THEN "true"
          {% elif max_result_threshold %}
          WHEN result < {{ max_result_threshold }} THEN "true"
          {% endif %}
          ELSE "false"
          END
        as value

    from ranked_values

    WHERE rank = 1

),

final as (

    {{ computed_field(slug=slug, type='lab_result', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
