
{% macro test_primary_equals_top_rank(model, ranked_model, primary_col, ranked_col, ranking_col) %}

  with primary as (

    select patientId, {{ primary_col }} as {{ ranked_col }}
    from {{ model }}

  ),

  top_ranked as (

    select patientId, {{ ranked_col }}
    from {{ ranked_model }}
    where {{ ranking_col }} = 1

  ),

  errors as (

    select *
    from primary
    left join top_ranked
    using (patientId)
    where primary.{{ ranked_col }} != top_ranked.{{ ranked_col }}

  )

  select count(*) from errors

{% endmacro %}
