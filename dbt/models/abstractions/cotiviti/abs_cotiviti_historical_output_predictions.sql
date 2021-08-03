with output_predictions as (

  select
    patientId,
    modelId,
    runId,
    predictionValue,
    createdAt
    
  from {{ source('cotiviti', 'output_predictions') }}

),

select_member_model_run as (

  select
    id,
    patientId,
    runId,
    modelId,
    modelName,
    ModelBasePeriodStart,
    ModelBasePeriodEnd
  from {{ ref('abs_cotiviti_member_selected_historical_runs') }}

),

final as (

  select
    id,
    patientId,
    modelId,
    runId,
    predictionValue,
    modelName,
    ModelBasePeriodStart,
    ModelBasePeriodEnd,
    createdAt
  from output_predictions
  
  inner join select_member_model_run
  using (patientId, runId, modelId)
)

select * from final
