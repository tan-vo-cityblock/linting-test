
{% set slugs = dbt_utils.get_column_values(table = ref('all_computed_fields'), column = 'fieldSlug') %}

{% set underscored_slugs = [] %}

with pivoted_results as (

  select 
    patientId,
    current_timestamp as updatedAt,

    {% for slug in slugs %}

      {% set underscored_slug = slug | replace("-", "_") %}
      {% set _ = underscored_slugs.append(underscored_slug) %}

      case 
        when fieldSlug = '{{ slug }}'
          then fieldValue 
      end as {{ underscored_slug }}

        {% if not loop.last %} , {% endif %}

    {% endfor %}

  from {{ ref('latest_computed_field_results') }}

),

final as (

  select 
    patientId,
    updatedAt,

    {% for slug in underscored_slugs %}

      array_agg({{ slug }} ignore nulls)[safe_offset(0)] as {{ slug }} 

        {% if not loop.last %} , {% endif %}

    {% endfor %}
    
  from pivoted_results

  group by patientId, updatedAt

)

select * from final

