with output_predictions as (
  
  select *
  from {{ source('cotiviti', 'output_predictions') }}
),

appendix_prediction_summary as (

  select runId, modelId, riskScoreType, riskScoreValue
  from {{ source('cotiviti', 'appendix_prediction_summary') }}
),

risk_model_info as (

  select modelId, predictionTime, predictionScoreType, predictionTargetEvent, predictionTargetExpenditureType
  from {{ source('cotiviti', 'model_info') }}
  where predictionScoreType = 'risk'
),

concurrent_expenditure_models as (

  select * from risk_model_info
  where predictionTargetExpenditureType is not null and predictionTime = 'concurrent'
),

prospective_expenditure_models as (

  select * from risk_model_info
  where predictionTargetExpenditureType is not null and predictionTime = 'prospective'
),

utilization_models as (

  select * from risk_model_info
  where predictionTargetEvent is not null
),

-- Concurrent expenditure risk scores will be normalized using the population average weighted by months eligible
concurrent_expenditure_predictions as (

  select *, true as isWeightedByMonthsEligible, riskScoreValue as populationAverageRRS
  from concurrent_expenditure_models
  inner join output_predictions using (modelId)
  inner join appendix_prediction_summary using (modelId, runId)
    where riskScoreType = 'Average Predictions, Weighted by Eligibility'
),

-- Prospective scores will be normalized using the unweighted population average, as the prediction is already annualized
prospective_expenditure_predictions as (

  select *, false as isWeightedByMonthsEligible, riskScoreValue as populationAverageRRS
  from prospective_expenditure_models
  inner join output_predictions using (modelId)
  inner join appendix_prediction_summary using (modelId, runId)
    where riskScoreType = 'Average Predictions, Not weighted by Eligibility'
),

utilization_predictions as (

  select *, false as isWeightedByMonthsEligible, riskScoreValue as populationAverageRRS
  from utilization_models
  inner join output_predictions using (modelId)
  inner join appendix_prediction_summary using (modelId, runId)
    where riskScoreType = 'Average Predictions, Not weighted by Eligibility'
),

all_risk_predictions_with_averages as (

  select * from concurrent_expenditure_predictions
    union all
  select * from prospective_expenditure_predictions
    union all
  select * from utilization_predictions
),

final as (

  select
    patientId,
    modelId,
    runId,
    predictionTime,
    predictionTargetEvent,
    predictionTargetExpenditureType,
    predictionValue as individualRRS,
    populationAverageRRS,
    isWeightedByMonthsEligible,
    predictionValue / populationAverageRRS as normalizedRRS,
    createdAt
  from all_risk_predictions_with_averages
)

select * from final
