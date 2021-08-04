with member_data as (

  SELECT
    omd.patientId,
    omd.runId,
    app.modelId,
    app.modelName,
    app.ModelBasePeriodStart,
    app.ModelBasePeriodEnd,
    mi.predictionScoreType,
    mi.predictionTime,
    mi.predictionTargetEvent,
    mi.predictionTargetExpenditureType,
    mi.population as modelTargetPopulation,
    xpt.population as memberPopulation,

    -- Facts to rank predictions by for each member
    case when mi.population = 'medicare' then 1
      when mi.population = 'medicaid' then 2
      when mi.population = 'commercial' then 3
    end as modelPopulationRank,
    case when mi.expenditureCapDollars = 100000 then 1
      when mi.expenditureCapDollars = 200000 then 2
      when mi.expenditureCapDollars = 250000 then 3
      when mi.expenditureCapDollars = 400000 then 4
      when mi.expenditureCapDollars is null then 5
      when mi.expenditureCapDollars = 25000 then 6
    end as  modelExpenditureCapRank,
    cast(mi.modelGroup = 'Risk Adjustment' as int64) as riskAdjustmentModelFlag,
    omd.createdAt,

    -- Forward-fill population (assume last known value is the population during a subsequent time period where population is missing)
    last_value(xpt.population ignore nulls) over (partition by xpt.patientId order by xpt.validFrom) as memberPopulation_ffill

  from {{ source('cotiviti', 'output_member_data') }} omd
  left join {{ source('cotiviti', 'appendix_general_run_info') }} app using (runId)
  left join {{ source('cotiviti', 'model_info') }} mi using (modelId)
  left join {{ ref('cotiviti_member') }} xpt
    on omd.patientId = xpt.patientId
      and app.ModelBasePeriodEnd between xpt.validFrom and xpt.validTo
),

member_data_with_target_population_info as (

  select
    *,

    -- Identify whether the member was not just in the target LOB for the model
    -- during any part of the model base period, but whether this was true
    -- at the end of the model base period
    -- NB: we use the forward-filled value of population
    -- NB: Dual members are to be included in medicare models
    -- NB: Since none of our Medicaid members are in FFS plans, we explicitly exclude those models
    case
      when modelName like '%Medicare FFS%' then false  -- We have no medicare FFS members
      else ((lower(memberPopulation_ffill) like '%' || modelTargetPopulation || '%') or
            (memberPopulation_ffill like '%Dual%' and modelTargetPopulation = 'medicare'))
    end as isTargetPopulationEnd

  from member_data
),

member_data_selected as (

  select *
  from member_data_with_target_population_info
  where isTargetPopulationEnd
),

-- Rank predictions according to model preferences to decide which prediction to keep for each prediction type
final as (

  select *,
    rank() over (
      partition by patientId, ModelBasePeriodStart, predictionScoreType, predictionTime, predictionTargetEvent, predictionTargetExpenditureType
      order by modelPopulationRank, riskAdjustmentModelFlag, modelExpenditureCapRank, ModelBasePeriodEnd desc, createdAt desc) as rnk
  from member_data_selected
)

select
  {{ dbt_utils.surrogate_key(['patientId', 'modelId', 'ModelBasePeriodStart', 'ModelBasePeriodEnd']) }} as id,
  patientId,
  runId,
  modelId,
  modelName,
  modelTargetPopulation,
  memberPopulation,
  ModelBasePeriodStart,
  ModelBasePeriodEnd,
  predictionScoreType,
  predictionTime,
  predictionTargetEvent,
  predictionTargetExpenditureType,
  createdAt

from final
  where rnk = 1
