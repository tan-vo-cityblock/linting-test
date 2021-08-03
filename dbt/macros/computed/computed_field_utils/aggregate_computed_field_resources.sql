{%- macro aggregate_computed_field_resources(table,
    min_field=None,
    min_value=None
  ) -%}

  select
  patientId,
  'true' as value,
  array_agg(struct(id, key, model, code, validDate)) as evaluatedResource

  from {{ table }}
  group by patientId
  {% if min_field and min_value %}
  having count(distinct {{ min_field }}) >= {{ min_value }}
  {% endif %}

{%- endmacro -%}
