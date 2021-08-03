{%- macro derive_computed_field_values(table) -%}

  select
  m.patientId,
  coalesce(r.value, 'false') as value,
  coalesce(r.evaluatedResource,
  [struct(
  string(null) as id,
  string(null) as key,
  string(null) as model,
  string(null) as code,
  date(null) as validDate)]) as evaluatedResource

  from {{ var('cf_member_table') }} m

  left join {{ table }} r
  using (patientId)

{%- endmacro -%}
