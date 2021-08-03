
with ccm_member_fields as (

  select
    patientId,
    complex_care_management,
    complex_care_management_one_required,
    age_85_or_older,
    ed_visits_three,
    ip_bh_admits_two,
    paid_amount_eighteen_thousand_medical,
    polypharmacy_chronic,
    spinal_cord_disorders_hcc_two_years,
    unstable_housing_assessment_one_year,
    complex_care_management_two_required,
    hiv_aids_hcc_two_year,
    dementia_w_comp_hcc_two_year,
    parkinsons_or_huntingtons_hcc_two_year,
    end_stage_liver_disease_cirrhosis_hcc_two_year,
    stroke_hcc_two_year,
    asthma_two_year,
    heart_failure,
    coronary_artery_disease,
    ckd_esrd_hcc_two_year,
    copd,
    chronic_pancreatitis_hcc_two_year,
    cystic_fibrosis_hcc_two_year,
    als_cp_muscular_dystrophy_hcc_two_year,
    multiple_sclerosis_hcc_two_year,
    morbid_obesity_hcc_two_year,
    has_diabetes,
    cancer_hcc_two_year,
    vascular_disease_hcc_two_year,
    post_organ_transplant_hcc_two_year,
    pressure_ulcer_stage_three_four_hcc_two_year,
    respiratory_failure_hcc_two_year,
    sickle_cell_disease_two_year,
    severe_head_injury_hcc_two_year,
    schizophrenia_hcc_two_year,
    major_depressive_hcc_two_year,
    substance_abuse_hcc_two_year

  from {{ ref('latest_computed_field_results_wide') }}

),

member as (

  select
    firstName,
    lastName,
    patientId,
    currentState,
    cohortName,
    partnerName,
    patientHomeMarketName as marketName,
    currentCareModel

  from {{ ref('member') }}
  
),

consent_dates as (

  select
    patientId,
    date(consentedAt, 'America/New_York') as consentDate

  from {{ ref('member_states') }}

),

final as (

  select
    m.firstName,
    m.lastName,
    cf.patientId as commonsId,
    m.currentState,
    cd.consentDate,
    m.cohortName,
    m.partnerName,
    m.marketName,
    m.currentCareModel,
    cf.* except(patientId)

  from ccm_member_fields cf

  left join member m
  using (patientId)

  left join consent_dates cd
  using (patientId)

)

select * from final
