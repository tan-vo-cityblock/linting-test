select
  id,
  patientId,
  modelId,
  runId,
  predictionValue as predictedLikelihoodOfHospitalization,
  modelName,
  ModelBasePeriodStart,
  ModelBasePeriodEnd,
  createdAt

from {{ ref('abs_cotiviti_latest_output_predictions') }}
where modelName like '%Hospitalization%'
