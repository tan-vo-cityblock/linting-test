with historical_risk_drivers as (

  select
    *,
    rank() over(partition by patientId, modelId, reportId order by ModelBasePeriodEnd desc, createdAt desc) as rnk
    
  from {{ ref('mrt_cotiviti_historical_individual_risk_drivers') }}

),

final as (

  select * except (rnk) from historical_risk_drivers
  where rnk = 1

)

select * from final
