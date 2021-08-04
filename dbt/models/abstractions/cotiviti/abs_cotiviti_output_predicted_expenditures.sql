with predictions_normalized as (

  select *, predictionTargetExpenditureType as expenditureType
  from {{ ref('abs_cotiviti_output_normalized_relative_risk_scores') }}
  where predictionTargetExpenditureType is not null
),

appendix_expenditures as (

  select
    runId,
    -- Add ' + ' between capitalized words, e.g. 'MedicalPharmacy' to 'Medical + Pharmacy'
    regexp_replace(expenditureType, '([a-z])([A-Z])', '\\1 + \\2') as expenditureType,
    averageExpenditureDollarsByPersonYears as populationAverageAnnualizedCostPMPY
  from {{ source ('cotiviti', 'appendix_expenditures') }}
),

-- Concurrent
-- Population Average PMPY Annualized Cost = Sum(Individual PMPY Annualized Cost * Eligf1) / Sum(Eligf1)
-- Individual Expected PMPY Cost = Individual Normalized RRS * Population Average PMPY Annualized Cost


-- Prospective
-- Population Average PMPY Annualized Cost = Sum(Annualized PMPY Individual Cost * Eligf1) /Sum(Eligf1)
-- Individual Expected PMPY Cost for Predicting Only Medical Risk = Individual Normalized RRS * (Population Average PMPY Annualized Medical Cost * (1+Medical Inflation))

final as (

  select
    patientId,
    modelId,
    predictionTime,
    expenditureType,
    individualRRS,
    populationAverageRRS,
    normalizedRRS,
    populationAverageAnnualizedCostPMPY,
    populationAverageAnnualizedCostPMPY * normalizedRRS as expectedAnnualizedCostPMPY,
    populationAverageAnnualizedCostPMPY / 12 as populationAverageCostPMPM,
    populationAverageAnnualizedCostPMPY * normalizedRRS / 12 as expectedCostPMPM,
    runId,
    createdAt
  from predictions_normalized
  left join appendix_expenditures using (runId, expenditureType)
)

select * from final
