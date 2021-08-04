
{% macro combine_and_create_unique_rankings(

    cte_list, 
    order_field = 'marketNames', 
    order_dir = 'asc',
    prefix = ''

  )
%}

  {{ prefix }}combined_rankings as (

    {% for cte in cte_list %}

      select * from {{ cte }}

        {% if not loop.last %}

          union all 

        {% endif %}

    {% endfor %}

  ),

  {{ prefix }}unique_rankings as (

    select
      patientId,
      nextStep,
      actionDueAt,
      rank() over (order by {{ order_field }} {{ order_dir }}, actionRank) as actionRank

    from {{ prefix }}combined_rankings

  ),

{% endmacro %}
