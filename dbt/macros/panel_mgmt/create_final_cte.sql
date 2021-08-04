
{% macro create_final_cte(table) %}
{# Given a preceding CTE name, pulls in rank of nextStep slug #}

  select
    generate_uuid() as id,
    patientId,
    nextStep,
    nsh.rank as nextStepRank,
    actionTiming,
    actionRank,
    current_timestamp as createdAt
  from {{ table }}
  left join {{ ref('next_step_hierarchy') }} nsh
  using (nextStep)

{% endmacro %}
