select
  patientId,

  {% for risk_area_slug in dbt_utils.get_column_values(table = source('commons', 'builder_risk_area'), column = 'slug') %}

    {%- set clean_slug = clean_slug(risk_area_slug) -%}

    max(if(goalRiskAreaSlug = '{{ risk_area_slug }}', true, false)) as has_{{ clean_slug }}_goal,
    max(if(goalRiskAreaSlug = '{{ risk_area_slug }}', goalCreatedAt, null)) as latest_{{ clean_slug }}_goal_created_at,

  {% endfor %}

from {{ ref('member_goals_tasks') }}
group by 1
