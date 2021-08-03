
{% macro boolean_fields(

    slug,
    condition_slugs,
    all_conditions = false,
    min_fields = 1,
    member_table = var('cf_member_table'),
    member_table_type = 'var',
    partner_name = None,
    partner_field = "partnerName",
    partner_op = "=",
    type = 'boolean_fields'

  )
%}

with 

  {{ find_members_w_field_value(condition_slugs, all_slugs = all_conditions, min_fields = min_fields) }}

  {{ finish_computed_field_query(
      slug,
      type,
      member_table = member_table,
      member_table_type = member_table_type,
      partner_name = partner_name,
      partner_field = partner_field,
      partner_op = partner_op
    )
  }}

{% endmacro %}
