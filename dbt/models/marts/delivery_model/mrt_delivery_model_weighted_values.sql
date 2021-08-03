{%- set flags_relation = ref('mrt_delivery_model_flags') -%}
{%- set delivery_model_screens = var('delivery_model_screens') -%}

with initial_assignments as (

  select
    patientId,
    
    case
      when
        {{ chain_or('', '', delivery_model_screens, quoted=False) }}
        then '{{ var('community_slug') }}'
      else '{{ var('virtual_slug') }}'
    end as initialModelAssignment

  from {{ flags_relation }}

),

unpivoted_values as (

  {{ dbt_utils.unpivot(
    relation=flags_relation,
    cast_to='float64',
    exclude=['patientId', 'marketName'],
    remove=delivery_model_screens,
    field_name='field',
    value_name='value'
    ) 
  }}

),

field_weights as (

  select field, weight
  from {{ ref('dta_delivery_model_fields') }}

),

field_stats as (

  select 
    field,
    avg(value) as avgValue, 
    stddev(value) as stdDevValue 
    
  from unpivoted_values
  group by 1

),

outlier_thresholds as (

  select *,
    avgValue + stdDevValue * 2 as avgPlusTwoStdDev
    
  from field_stats
  
),

outlier_flags as (

  select
    {{ dbt_utils.surrogate_key(['patientId', 'field']) }} as id,
    u.*,
    o.avgPlusTwoStdDev,
    u.value > o.avgPlusTwoStdDev as isGreaterThanTwoStdDev
    
  from unpivoted_values u

  left join outlier_thresholds o
  using (field)
  
),

scaled_values as (

  select *,
    
    case
      when isGreaterThanTwoStdDev
        then 1
      else safe_divide(value, avgPlusTwoStdDev)
    end as scaledValue
    
  from outlier_flags
  
),

final as (

  select
    s.id,
    s.patientId,
    s.marketName,
    i.initialModelAssignment,
    s.* except (id, patientId, marketName),
    coalesce(s.scaledValue * w.weight, 0) as weightedValue

  from scaled_values s

  left join initial_assignments i
  using (patientId)

  left join field_weights w
  using (field)

)

select * from final
