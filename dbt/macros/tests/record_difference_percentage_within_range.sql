{%- macro test_record_difference_percentage_within_range(
      model,
      has_line_of_business_totals=false,
      control_total_source=source('emblem_silver_v2', 'control_v2_*'),
      control_total_file_name_root=None,
      minimum_allowed_difference_percentage=-0.001,
      maximum_allowed_difference_percentage=0
    ) 
-%}

with actual_shard_totals as (

  select
    _table_suffix as tableSuffix,
    count(*) as actualTotal,
    max(_table_suffix) over() as maxTableSuffix
    
  from {{ model }}

  {% if has_line_of_business_totals %}

    where patient.patientId is not null

  {% endif %}

  group by 1

),

{# assumes that each data.FILENAME value appears in a single row within each shard #}
control_shard_totals as (

  select
    _table_suffix as tableSuffix,
    
    {% if has_line_of_business_totals %}

      {% for line_of_business in ['HIP', 'GHI'] %}

        coalesce(safe_cast(data.{{ line_of_business }}_COUNT as int64), 0) {% if not loop.last %} + {% endif %}

      {% endfor %}

      as controlTotal,
    
    {% else %}
      
      safe_cast(data.TOTAL_COUNT as int64) as controlTotal,

    {% endif %}

    max(_table_suffix) over() as maxTableSuffix
    
  from {{ control_total_source }}
  where data.FILENAME like '%{{ control_total_file_name_root }}%'

),

latest_actual_total as (

  select tableSuffix, actualTotal
  from actual_shard_totals
  where tableSuffix = maxTableSuffix

),

latest_control_total as (

  select tableSuffix, controlTotal
  from control_shard_totals
  where tableSuffix = maxTableSuffix

),

record_difference_percentage as (

  select
    safe_divide((actualTotal - controlTotal), controlTotal) as recordDifferencePercentage
    
  from latest_actual_total

  {# assumes that latest table shard and latest control shard have same table suffix #}
  inner join latest_control_total
  using (tableSuffix)
  
),

errors as (

  {# requires exactly one passing result #}
  select 1 - count(*)
  from record_difference_percentage
  where
    recordDifferencePercentage between
      {{ minimum_allowed_difference_percentage }} and {{ maximum_allowed_difference_percentage }}
)

select * from errors

{%- endmacro -%}
