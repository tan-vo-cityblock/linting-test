with historical_predictions as (

  select
    *,
    rank() over(partition by patientId, modelId order by ModelBasePeriodEnd desc, createdAt desc) as rnk
    
  from {{ ref('abs_cotiviti_historical_output_predictions') }}

),

final as (

  select * except (rnk) from historical_predictions
  where rnk = 1

)

select * from final
