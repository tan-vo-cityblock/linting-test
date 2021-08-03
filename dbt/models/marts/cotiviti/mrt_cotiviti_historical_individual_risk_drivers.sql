with individual_risk_drivers as (

  select
    patientId,
    runId,
    reportId,
    rank,
    ClinicalConditionCategory as clinicalConditionCategory,
    riskContributionPercent,
    createdAt
  from {{ source('cotiviti', 'report_individual_risk_drivers') }}
),

historical_predictions as (

  select
    id as cotivitiPredictionId,
    patientId,
    modelId,
    runId,
    ModelBasePeriodStart,
    ModelBasePeriodEnd
  from {{ ref('abs_cotiviti_historical_output_predictions') }}
),

final as (

  select
    {{ dbt_utils.surrogate_key(['cotivitiPredictionId', 'rank']) }} as id,
    hp.cotivitiPredictionId,
    ird.patientId,
    ird.runId,
    ird.reportId,
    hp.modelId,
    hp.ModelBasePeriodStart,
    hp.ModelBasePeriodEnd,
    ird.rank,
    ird.clinicalConditionCategory,
    ird.riskContributionPercent,
    ird.createdAt

  from individual_risk_drivers ird

  inner join historical_predictions hp
    using (patientId, runId)
)

select * from final
