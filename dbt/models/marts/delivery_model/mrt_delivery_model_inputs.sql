{%- set encounter_configs = var('encounter_configs') -%}
{%- set year_list = ['1', '2', '3'] -%}
{%- set encounter_trend_list = [] -%}

{%- for encounter in encounter_configs -%}

  {%- set count_root = encounter['count_root'] -%}

  {%- if count_root in ['EdVisit', 'AcsEdVisit', 'UnplannedInpatientAdmit', 'AcsInpatient', 'Readmission'] -%}

    {%- set _ = encounter_trend_list.append('rollingTwelveMonth' ~ count_root ~ 'Count') -%}

  {%- endif -%}

    {%- if count_root in ['EdVisit', 'InpatientAdmit'] -%}

      {%- for year in year_list -%}

        {%- set _ = encounter_trend_list.append('annual' ~ encounter['count_root'] ~ 'CountYear' ~ year) -%}

      {%- endfor -%}

    {%- endif -%}

{%- endfor -%}

{%- for year in year_list -%}

  {%- set _ = encounter_trend_list.append('planPaidDollarsPerMemberMonthYear' ~ year) -%}

{%- endfor -%}

with members as (

  select
  patientId,
  patientHomeMarketName as marketName

  from {{ ref('abs_commons_member_market_clinic') }}

  left join {{ ref('member_states') }}
  using (patientId)
  where patientHomeMarketName in ('New York City', 'Connecticut', 'Massachusetts','CareFirst DC', 'North Carolina')
  and currentState not like 'disenrolled%'

),

member_hcc_groups as (

  select
  patientId,
  grphcc_dol_d3_1 || coalesce(grphcc_dol_d3_2, '') as hccCombined

  from {{ ref('member_hcc_groups') }}
  where grphcc_dol_d3_1 is not null

),

cf_dialysis as (

  select
  patientId

  from {{ ref('cf_dialysis') }}
  where fieldValue = 'true'

),

pivoted_cost_classification_totals as (

  select
  patientId,
  medicalPaidDollars,
  dialysisPaidDollars,
  snfPaidDollars,
  dmePaidDollars,
  bhPaidDollars

  from {{ ref('mrt_claims_pivoted_cost_classification_totals') }}

),

member_eligibility as (

  select
  patientId,
  memberMonths

  from {{ ref('mrt_member_eligibility_rolling_twelve_months') }}

),

cf_palliative as (

  select
  patientId

  from {{ ref('cf_palliative') }}
  where fieldValue = 'true'

),

cf_complex_care_management as (

  select
  patientId

  from {{ ref('cf_complex_care_management') }}
  where fieldValue = 'true'

),

cf_complex_care_management_pediatric as (

  select
  patientId

  from {{ ref('cf_complex_care_management_pediatric') }}
  where fieldValue = 'true'

),

medication_counts as (

  select
  patientId,
  ndcCount

  from {{ ref('mrt_medical_medication_counts') }}

),

pivoted_annual_medical_costs as (

  select *
  from {{ ref('mrt_claims_pivoted_annual_medical_costs') }}

),

chronic_condition_counts as (

  select
  patientId,
  chronicConditionCount

  from {{ ref('mrt_medical_chronic_condition_counts') }}

),

pivoted_twelve_month_encounters as (

  select
  patientId,
  {{ mcr_claims_pivot_encounters(encounter_configs, count_prefix='rollingTwelveMonth') }}

  from {{ ref('mrt_claims_rolling_twelve_months') }}
  group by 1

),

pivoted_annual_encounters as (

  select *

  from {{ ref('mrt_claims_pivoted_annual_encounters') }}

),

mrt_cotiviti_latest_output_predictions as (

  select
  patientId,
  predictedLikelihoodOfHospitalization as cotivitiLikelihoodOfHospitalizationInNextSixMonths,
  rank() over(partition by patientId order by runId desc) as rnk

  from {{ ref('mrt_cotiviti_latest_likelihood_of_hospitalization') }}

),

cotiviti as (

  select * except (rnk)

  from mrt_cotiviti_latest_output_predictions
  where rnk = 1

),

members_w_zero_paid_dollars_in_prev_yr as (

  select
  patientId

  from pivoted_annual_medical_costs
  where planPaidDollarsPerMemberMonthYear3 = 0

),

members_w_no_claim_history as (

  select
  patientId

  from members
  except distinct
  select patientId
  from pivoted_annual_medical_costs

),

inputs as (

  select
  m.patientId,
  m.marketName,

  {% for condition in
    ['Dementia', 'Dialysis', 'Parkinsons/Huntingtons', 'Quadraplegia/Paraplegia/Amputation/Hemiplegia']
  %}

  case
    when mhg.hccCombined like '%{{ condition }}%'
    then true
    else false
  end as topTwoHccsInclude{{ condition.replace("/", "Or") }},

{% endfor %}

  cfd.patientId is not null as pathwayFieldIndicatesDialysis,
  cfp.patientId is not null as hasEvidenceOfAdvancedIllness,
  case
    when ccm.patientId is not null or ccmp.patientId is not null
    then true
    else false
  end as hasEvidenceOfCcm,
  (zero.patientId is not null or no_claims.patientId is not null) as hasZeroMedicalCostInPriorYearOrNoClaimsHistory,

  {% for field in ['chronicConditionCount', 'ndcCount', 'cotivitiLikelihoodOfHospitalizationInNextSixMonths'] %}

    coalesce({{ field }}, 0) as {{ field }},

  {% endfor %}

  {% for field in ['dialysisPaidDollars', 'medicalPaidDollars', 'snfPaidDollars', 'dmePaidDollars', 'bhPaidDollars'] %}

    coalesce(safe_divide({{ field }}, memberMonths), 0) as {{ field }}PerMonth,

  {% endfor %}

  {% for field in encounter_trend_list %}

    coalesce({{ field }}, 0) as {{ field }},

  {% endfor %}

  from members m

  left join member_hcc_groups mhg
  using (patientId)

  left join cf_dialysis cfd
  using (patientId)

  left join pivoted_cost_classification_totals pcct
  using (patientId)

  left join member_eligibility me
  using (patientId)

  left join cf_palliative cfp
  using (patientId)

  left join cf_complex_care_management ccm
  using (patientId)

  left join cf_complex_care_management_pediatric ccmp
  using (patientId)

  left join medication_counts mc
  using (patientId)

  left join pivoted_annual_medical_costs pamc
  using (patientId)

  left join chronic_condition_counts ccc
  using (patientId)

  left join pivoted_twelve_month_encounters
  using (patientId)

  left join pivoted_annual_encounters
  using (patientId)

  left join cotiviti c
  using (patientId)

  left join members_w_zero_paid_dollars_in_prev_yr zero
  using (patientId)

  left join members_w_no_claim_history no_claims
  using (patientId)

)

select *
from inputs
