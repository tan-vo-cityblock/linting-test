
{% macro convert_acuity_score_to_description(acuity_col = 'score') %}

  case
    when {{ acuity_col }} = 0 then 'No Acuity'
    when {{ acuity_col }} = 1 then 'Stable'
    when {{ acuity_col }} = 2 then 'Mild'
    when {{ acuity_col }} = 3 then 'Moderate'
    when {{ acuity_col }} = 4 then 'Severe'
    when {{ acuity_col }} = 5 then 'Critical'
  end

{% endmacro %}
