{%- set cte_list = [] -%}

with pathway_fields as (

  select
    slug as pathwaySlug,
    computedFieldSlug as fieldSlug,
    0 as fieldLevel

  from {{ ref('pathway_fields') }}

),

input_fields as (

  select
    slug as fieldSlug,
    codes as inputFieldSlug
   
  from {{ ref('computed_codes') }}
  where
    parameter = 'condition_slugs' and
    operator = 'included'

),

{% for level in var('supported_pathway_field_levels') %}

  {%- set cte_name = 'level_' ~ level ~ '_inputs' -%}
  {%- set _ = cte_list.append(cte_name) -%}

  {{ cte_name }} as (

    select
      p.pathwaySlug,
      i.inputFieldSlug as fieldSlug,
      {{ level }} as fieldLevel

      {% if loop.first %}

        from pathway_fields p

      {% else %}

        from level_{{ level - 1 }}_inputs p

      {% endif %}

    inner join input_fields i
    using (fieldSlug)

  ),

{% endfor %}

all_fields as (

  select * from pathway_fields
  union all

  {{ union_cte_list(cte_list, union_type = 'all') }}

),

final as (

  select
    {{ dbt_utils.surrogate_key(['pathwaySlug', 'fieldSlug', 'fieldLevel']) }} as id,
    *

  from all_fields

)

select * from final
