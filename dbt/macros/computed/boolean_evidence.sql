{%- macro boolean_evidence(slug,
      condition_slugs,
      min_conditions=1,
      excluded_condition_slugs=None,
      all_conditions=False
    ) -%}

{%- set included_cte_list = [] -%}
{%- set excluded_cte_list = [] -%}

with

{% for slug in condition_slugs %}

  {%- set clean_slug = clean_slug(slug) -%}
  {%- set _ = included_cte_list.append('members_with_' ~ clean_slug) -%}

    members_with_{{ clean_slug }} as (

      select * except(id)

      from {{ slug_to_ref(slug) }}
      where fieldValue = 'true'

),

{% endfor %}

{%- if excluded_condition_slugs -%}

  {% for slug in excluded_condition_slugs %}

    {%- set clean_slug = clean_slug(slug) -%}
    {%- set _ = excluded_cte_list.append('members_with_' ~ clean_slug) -%}

    members_with_{{ clean_slug }} as (

      select
      patientId

      from {{ slug_to_ref(slug) }}
      where fieldValue = 'true'

),
  {% endfor %}

members_with_excluded_conditions as (

  {{ union_cte_list(excluded_cte_list, union_type = 'all') }}

),

{%- endif -%}

member_features as (

{{ union_cte_list(included_cte_list, union_type = 'all') }}

),

members_with_included_conditions as (

  {% if all_conditions == True %}
  {{ join_cte_list(included_cte_list, type='inner') }}

  {% else %}
  select
  distinct patientId

  from member_features

  {%- endif -%}

),

evidence as (

  select *

  from members_with_included_conditions
  left join member_features using (patientId)
  left join unnest (evaluatedResource) as evaluatedResource

  {% if excluded_condition_slugs %}
  where patientId not in (select patientId from members_with_excluded_conditions)
  {% endif %}

),

aggregated_evidence as (

  {{ aggregate_computed_field_resources(table='evidence') }}

),

cf_status as (

  {{ derive_computed_field_values(table='aggregated_evidence') }}

),

final as (

  {{ computed_field(slug=slug, type='boolean_evidence', value='value', table='cf_status', evaluated_resource='evaluatedResource') }}

)

select *
from final

{%- endmacro -%}
