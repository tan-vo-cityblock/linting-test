with risk_drivers_hcc as (

  select patientId, modelId, runId, HCC, riskContributionPercent
  from {{ source('cotiviti', 'output_risk_drivers_hcc') }}
),

hcc_to_buckets as (

  select
    HCC_Seq,
    avg(percentPhysical) as percentPhysical,
    avg(percentBehavioral) as percentBehavioral,
    avg(percentSocial) as percentSocial
  from {{ source('cotiviti', 'dxcg_clinical_classification_v7') }}
  group by 1
),

not_normalized as (

  select
    patientId,
    modelId,
    runId,
    sum(riskContributionPercent * percentPhysical) as riskContributionPhysicalPercent,
    sum(riskContributionPercent * percentBehavioral) as riskContributionBehavioralPercent,
    sum(riskContributionPercent * percentSocial) as riskContributionSocialPercent,
    sum(riskContributionPercent) as riskContributionTotalPercent
  from risk_drivers_hcc rdh
  left join hcc_to_buckets htb on rdh.HCC = htb.HCC_Seq
  group by 1, 2, 3
),

final as (

  select
    patientId,
    modelId,
    runId,
    riskContributionPhysicalPercent,
    riskContributionBehavioralPercent,
    riskContributionSocialPercent,
    riskContributionTotalPercent,
    riskContributionPhysicalPercent * 100 / riskContributionTotalPercent as normalizedRiskContributionPhysicalPercent,
    riskContributionBehavioralPercent * 100 / riskContributionTotalPercent as normalizedRiskContributionBehavioralPercent,
    riskContributionSocialPercent * 100 / riskContributionTotalPercent as normalizedRiskContributionSocialPercent
  from not_normalized
)

select * from final
