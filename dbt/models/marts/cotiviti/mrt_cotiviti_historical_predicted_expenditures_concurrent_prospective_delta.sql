with all_expenditure_predictions as (

  select *
  from {{ ref('abs_cotiviti_output_predicted_expenditures') }} exp
),

selected_historical_runs as (
  select id as predictionId, modelId, runId, patientId, modelTargetPopulation, ModelBasePeriodStart, ModelBasePeriodEnd
  from {{ ref('abs_cotiviti_member_selected_historical_runs') }}
),

member_market as (
  select patientId, patientHomeMarketName
  from {{ ref('member') }}
),

selected_expenditures as (

  select 
    patientId,
    predictionId,
    runId,
    modelId,
    modelTargetPopulation,
    aep.predictionTime,
    expenditureType,
    expectedCostPMPM,
    ModelBasePeriodStart,
    ModelBasePeriodEnd,
    patientHomeMarketName
  from all_expenditure_predictions aep
  inner join selected_historical_runs shi using (patientId, modelId, runId)
  left join member_market using (patientId)
  
),

prospective_concurrent as (

    select
      pros.patientId,
      pros.patientHomeMarketName,
      pros.ModelBasePeriodStart,
      pros.ModelBasePeriodEnd,
      pros.modelId as prospectiveModelId,
      pros.predictionId as prospectivePredictionId,
      conc.modelId as concurrentModelId,
      conc.predictionId as concurrentPredictionId,
      pros.modelTargetPopulation,
      pros.expenditureType,
      pros.expectedCostPMPM as prospectiveExpectedCostPMPM,
      conc.expectedCostPMPM as concurrentExpectedCostPMPM,
      pros.expectedCostPMPM - conc.expectedCostPMPM as prospectiveConcurrentCostDeltaPMPM
    from selected_expenditures pros
    left join selected_expenditures conc 
      on pros.patientId = conc.patientId
        and pros.expenditureType = conc.expenditureType
        and pros.modelTargetPopulation = conc.modelTargetPopulation
        and pros.ModelBasePeriodStart = conc.ModelBasePeriodStart
        and pros.predictionTime = 'prospective'
        and conc.predictionTime = 'concurrent'
),

final as (

    select
      *,
      case when prospectiveExpectedCostPMPM is null then null
        else percent_rank() over (
          partition by expenditureType, patientHomeMarketName, ModelBasePeriodStart, prospectiveExpectedCostPMPM is null
          order by prospectiveExpectedCostPMPM desc) end as prospectiveCostPercentRank,
      case when concurrentExpectedCostPMPM is null then null
        else percent_rank() over (
          partition by expenditureType, patientHomeMarketName, ModelBasePeriodStart, concurrentExpectedCostPMPM is null
          order by concurrentExpectedCostPMPM desc) end as concurrentCostPercentRank,
      case when prospectiveConcurrentCostDeltaPMPM is null then null
        else percent_rank() over (
          partition by expenditureType, patientHomeMarketName, ModelBasePeriodStart, prospectiveConcurrentCostDeltaPMPM is null
          order by prospectiveConcurrentCostDeltaPMPM desc) end as prospectiveConcurrentCostDeltaPercentRank
    from prospective_concurrent
)

select 
  patientId,
  patientHomeMarketName,
  ModelBasePeriodStart,
  ModelBasePeriodEnd,
  modelTargetPopulation,
  expenditureType,
  prospectiveModelId,
  prospectivePredictionId,
  concurrentModelId,
  concurrentPredictionId,
  prospectiveExpectedCostPMPM,
  concurrentExpectedCostPMPM,
  prospectiveConcurrentCostDeltaPMPM,
  prospectiveCostPercentRank,
  concurrentCostPercentRank,
  prospectiveConcurrentCostDeltaPercentRank
from final
