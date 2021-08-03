with historical_expenditures as (

  select
    *,
    rank() over(partition by patientId, expenditureType order by ModelBasePeriodEnd desc) as rnk
    
  from {{ ref('mrt_cotiviti_historical_predicted_expenditures_concurrent_prospective_delta') }}

),

final as (

  select * except (rnk) from historical_expenditures
  where rnk = 1

)

select * from final
