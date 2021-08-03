{%- macro computed_field(slug,
    type,
    value,
    table,
    evaluated_resource=None
  ) -%}

  select
  generate_uuid() as id,
  patientId,
  '{{ slug }}' as fieldSlug,
  '{{ type }}' as fieldType,
  {{ value }} as fieldValue,

  {% if evaluated_resource %}
  {{ evaluated_resource }} as evaluatedResource,
  {% else %}
  [struct(
    string(null) as id,
    string(null) as key,
    string(null) as model,
    string(null) as code,
    date(null) as validDate)] as evaluatedResource,
  {% endif %}

  current_timestamp as createdAt

  from {{ table }}

{%- endmacro -%}
