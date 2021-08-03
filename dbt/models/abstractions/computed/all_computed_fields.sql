
select * from {{ ref('cf_a_1_c_lab') }}
union all
select * from {{ ref('cf_a_1_c_level') }}
union all
select * from {{ ref('cf_a_1_c_pre_diabetes') }}
union all
select * from {{ ref('cf_a_1_c_proc') }}
union all
select * from {{ ref('cf_ace_arbs') }}
union all
select * from {{ ref('cf_acs_ed_visits_two') }}
union all
select * from {{ ref('cf_acs_ip_or_unplanned_admits_two') }}
union all
select * from {{ ref('cf_acs_bh_or_unplanned_admits_three') }}
union all
select * from {{ ref('cf_acs_ip_or_unplanned_admits_two_or_acs_ed_visits_two') }}
union all
select * from {{ ref('cf_active_cancer_hcc') }}
union all
select * from {{ ref('cf_active_cancer_w_chemo') }}
union all
select * from {{ ref('cf_acute_mi') }}
union all
select * from {{ ref('cf_acute_utilization_level') }}
union all
select * from {{ ref('cf_advanced_illness') }}
union all
select * from {{ ref('cf_advanced_illness_severe') }}
union all
select * from {{ ref('cf_age_18_or_older') }}
union all
select * from {{ ref('cf_age_18_or_older_w_one_covid_risk_factor_younger') }}
union all
select * from {{ ref('cf_age_18_or_older_w_one_covid_risk_factor_critical') }}
union all
select * from {{ ref('cf_age_18_or_older_w_active_cancer_and_chemo') }}
union all
select * from {{ ref('cf_age_20_or_younger') }}
union all
select * from {{ ref('cf_age_21_or_older') }}
union all
select * from {{ ref('cf_age_50_or_older') }}
union all
select * from {{ ref('cf_age_50_or_older_w_one_covid_risk_factor') }}
union all
select * from {{ ref('cf_age_55_or_older') }}
union all
select * from {{ ref('cf_age_60_or_older') }}
union all
select * from {{ ref('cf_age_60_or_older_w_two_covid_risk_factors') }}
union all
select * from {{ ref('cf_age_65_or_older') }}
union all
select * from {{ ref('cf_age_80_or_older') }}
union all
select * from {{ ref('cf_age_80_or_older_w_one_covid_risk_factor') }}
union all
select * from {{ ref('cf_age_85_or_older') }}
union all
select * from {{ ref('cf_alcohol_dependence_not_in_remission') }}
union all
select * from {{ ref('cf_als') }}
union all
select * from {{ ref('cf_als_cp_muscular_dystrophy_hcc_two_year') }}
union all
select * from {{ ref('cf_angina') }}
union all
select * from {{ ref('cf_aspiration_pneumonia') }}
union all
select * from {{ ref('cf_asthma') }}
union all
select * from {{ ref('cf_asthma_uncontrolled') }}
union all
select * from {{ ref('cf_asthma_ever') }}
union all
select * from {{ ref('cf_asthma_two_year') }}
union all
select * from {{ ref('cf_attention_deficit_hyperactivity_disorders_two_year') }}
union all
select * from {{ ref('cf_audit_assessment') }}
union all
select * from {{ ref('cf_autism_two_year') }}
union all
select * from {{ ref('cf_autonomic_dysfunction') }}
union all
select * from {{ ref('cf_behavioral_conduct_disorders_two_year') }}
union all
select * from {{ ref('cf_bipolar_disorder') }}
union all
select * from {{ ref('cf_blindness_two_year') }}
union all
select * from {{ ref('cf_bmi_35') }}
union all
select * from {{ ref('cf_bmi_25_vitals') }}
union all
select * from {{ ref('cf_bmi_25_dx') }}
union all
select * from {{ ref('cf_bmi_25_vitals_or_dx') }}
union all
select * from {{ ref('cf_bmi_35_and_diabetes_hcc') }}
union all
select * from {{ ref('cf_bmi_40') }}
union all
select * from {{ ref('cf_bmi_40_not_diabetes_hcc') }}
union all
select * from {{ ref('cf_bp_level') }}
union all
select * from {{ ref('cf_bp_presence') }}
union all
select * from {{ ref('cf_bp_presence_4_month') }}
union all
select * from {{ ref('cf_cabg') }}
union all
select * from {{ ref('cf_cad_cvd_ever') }}
union all
select * from {{ ref('cf_cancer') }}
union all
select * from {{ ref('cf_cancer_hcc_two_year') }}
union all
select * from {{ ref('cf_cannabis_dependence_not_in_remission') }}
union all
select * from {{ ref('cf_celiac_disease_two_year') }}
union all
select * from {{ ref('cf_cerebral_palsy') }}
union all
select * from {{ ref('cf_cerebral_palsy_two_year') }}
union all
select * from {{ ref('cf_cerebrovascular_disease') }}
union all
select * from {{ ref('cf_check_a_1_c') }}
union all
select * from {{ ref('cf_chemotherapy') }}
union all
select * from {{ ref('cf_chronic_lung_disease_two_year') }}
union all
select * from {{ ref('cf_chronic_pancreatitis_hcc_two_year') }}
union all
select * from {{ ref('cf_cirrhosis') }}
union all
select * from {{ ref('cf_ckd') }}
union all
select * from {{ ref('cf_ckd_esrd_hcc_two_year') }}
union all
select * from {{ ref('cf_ckd_assessment') }}
union all
select * from {{ ref('cf_ckd_esrd_assessment') }}
union all
select * from {{ ref('cf_ckd_stage_3') }}
union all
select * from {{ ref('cf_ckd_stage_4') }}
union all
select * from {{ ref('cf_ckd_stage_5') }}
union all
select * from {{ ref('cf_cmm_risk_factors') }}
union all
select * from {{ ref('cf_cocaine_dependence_not_in_remission') }}
union all
select * from {{ ref('cf_complex_care_management') }}
union all
select * from {{ ref('cf_complex_care_management_one_required') }}
union all
select * from {{ ref('cf_complex_care_management_pediatric') }}
union all
select * from {{ ref('cf_complex_care_management_two_required') }}
union all
select * from {{ ref('cf_complex_home_equipment') }}
union all
select * from {{ ref('cf_comprehensive_assessment_ccm') }}
union all
select * from {{ ref('cf_comprehensive_assessment_ccm_pediatric') }}
union all
select * from {{ ref('cf_comprehensive_medication_management') }}
union all
select * from {{ ref('cf_congenital_malformations_deformations_chromosomal_abnormalities_two_year') }}
union all
select * from {{ ref('cf_copd') }}
union all
select * from {{ ref('cf_copd_ever') }}
union all
select * from {{ ref('cf_coronary_artery_disease') }}
union all
select * from {{ ref('cf_coronary_artery_disease_ever') }}
union all
select * from {{ ref('cf_covid_members_forced_critical') }}
union all
select * from {{ ref('cf_covid_members_forced_severe') }}
union all
select * from {{ ref('cf_covid_risk_factors') }}
union all
select * from {{ ref('cf_covid_risk_factors_critical') }}
union all
select * from {{ ref('cf_covid_risk_factors_younger') }}
union all
select * from {{ ref('cf_covid_risk_level') }}
union all
select * from {{ ref('cf_current_alcohol_abuse') }}
union all
select * from {{ ref('cf_current_opioid_abuse') }}
union all
select * from {{ ref('cf_current_other_substance_abuse') }}
union all
select * from {{ ref('cf_current_substance_use_not_treatments') }}
union all
select * from {{ ref('cf_cvd') }}
union all
select * from {{ ref('cf_cvd_hcc_ever') }}
union all
select * from {{ ref('cf_cvd_on_high_mod_statin') }}
union all
select * from {{ ref('cf_cystic_fibrosis_ever') }}
union all
select * from {{ ref('cf_cystic_fibrosis_hcc_two_year') }}
union all
select * from {{ ref('cf_dast_assessment') }}
union all
select * from {{ ref('cf_deafness_two_year') }}
union all
select * from {{ ref('cf_dementia') }}
union all
select * from {{ ref('cf_dementia_meds') }}
union all
select * from {{ ref('cf_dementia_w_comp_hcc_two_year') }}
union all
select * from {{ ref('cf_depression') }}
union all
select * from {{ ref('cf_detox_treatment') }}
union all
select * from {{ ref('cf_detox_treatment_4_months') }}
union all
select * from {{ ref('cf_diabetes_a_1_c') }}
union all
select * from {{ ref('cf_diabetes_hcc') }}
union all
select * from {{ ref('cf_diabetes_hcc_two_year') }}
union all
select * from {{ ref('cf_diabetes_on_statin') }}
union all
select * from {{ ref('cf_diabetes_poorly_controlled') }}
union all
select * from {{ ref('cf_diabetic_retinal_screening') }}
union all
select * from {{ ref('cf_diabetic_retinal_screening_dm_wo_comp') }}
union all
select * from {{ ref('cf_diabetic_retinal_screening_negative') }}
union all
select * from {{ ref('cf_diabetic_retinal_screening_negative_prior') }}
union all
select * from {{ ref('cf_diabetic_retinal_screening_w_eye_care_professional') }}
union all
select * from {{ ref('cf_dialysis') }}
union all
select * from {{ ref('cf_dialysis_adherence_problems') }}
union all
select * from {{ ref('cf_dm_adequately_controlled') }}
union all
select * from {{ ref('cf_dm_moderately_controlled') }}
union all
select * from {{ ref('cf_dm_needs_eye_exam') }}
union all
select * from {{ ref('cf_dm_needs_nephropathy_screen') }}
union all
select * from {{ ref('cf_dysphagia') }}
union all
select * from {{ ref('cf_eating_disorder') }}
union all
select * from {{ ref('cf_eating_disorder_two_year') }}
union all
select * from {{ ref('cf_ed_visits_three') }}
union all
select * from {{ ref('cf_ed_visits_four') }}
union all
select * from {{ ref('cf_ed_visits_five') }}
union all
select * from {{ ref('cf_elevated_historical_bp') }}
union all
select * from {{ ref('cf_entitlement_gaps') }}
union all
select * from {{ ref('cf_epilepsy') }}
union all
select * from {{ ref('cf_epilepsy_recurrent_seizures_convulsions_two_year') }}
union all
select * from {{ ref('cf_esrd') }}
union all
select * from {{ ref('cf_esrd_assessment') }}
union all
select * from {{ ref('cf_esrd_current') }}
union all
select * from {{ ref('cf_esrd_narrow') }}
union all
select * from {{ ref('cf_end_stage_liver_disease') }}
union all
select * from {{ ref('cf_end_stage_liver_disease_cirrhosis_hcc_two_year') }}
union all
select * from {{ ref('cf_estrogen_agonists') }}
union all
select * from {{ ref('cf_exposure_to_violence') }}
union all
select * from {{ ref('cf_failure_to_thrive_two_year') }}
union all
select * from {{ ref('cf_falls') }}
union all
select * from {{ ref('cf_falls_age_60_or_older_no_dementia') }}
union all
select * from {{ ref('cf_female_members') }}
union all
select * from {{ ref('cf_food_insecurity') }}
union all
select * from {{ ref('cf_frailty') }}
union all
select * from {{ ref('cf_frailty_2_day') }}
union all
select * from {{ ref('cf_glucose_fasting_pre_diabetes') }}
union all
select * from {{ ref('cf_gestational_diabetes_ever') }}
union all
select * from {{ ref('cf_hallucinogen_dependence_not_in_remission') }}
union all
select * from {{ ref('cf_has_anxiety_diagnosis') }}
union all
select * from {{ ref('cf_has_diabetes') }}
union all
select * from {{ ref('cf_has_diabetes_type_1') }}
union all
select * from {{ ref('cf_has_diabetes_type_2') }}
union all
select * from {{ ref('cf_has_pre_diabetes') }}
union all
select * from {{ ref('cf_has_pre_diabetes_all') }}
union all
select * from {{ ref('cf_has_pre_diabetes_any') }}
union all
select * from {{ ref('cf_heart_failure') }}
union all
select * from {{ ref('cf_heart_failure_severe') }}
union all
select * from {{ ref('cf_heart_failure_severe_diagnoses') }}
union all
select * from {{ ref('cf_heart_failure_w_oxygen') }}
union all
select * from {{ ref('cf_hepatitis_b') }}
union all
select * from {{ ref('cf_high_hemoglobin_a1c') }}
union all
select * from {{ ref('cf_hip_fractures') }}
union all
select * from {{ ref('cf_hip_fractures_age_65_or_older') }}
union all
select * from {{ ref('cf_history_of_alcohol') }}
union all
select * from {{ ref('cf_history_of_opiates') }}
union all
select * from {{ ref('cf_history_of_other_substances') }}
union all
select * from {{ ref('cf_hiv') }}
union all
select * from {{ ref('cf_hiv_aids_hcc_two_year') }}
union all
select * from {{ ref('cf_homelessness_diagnoses') }}
union all
select * from {{ ref('cf_homelessness_assessment') }}
union all
select * from {{ ref('cf_homelessness') }}
union all
select * from {{ ref('cf_housing') }}
union all
select * from {{ ref('cf_housing_conditions') }}
union all
select * from {{ ref('cf_housing_member_list') }}
union all
select * from {{ ref('cf_hydrocephalus_two_year') }}
union all
select * from {{ ref('cf_hypertension') }}
union all
select * from {{ ref('cf_hypertension_current_or_prior') }}
union all
select * from {{ ref('cf_hypertension_acuity') }}
union all
select * from {{ ref('cf_hypertension_admission_30_day') }}
union all
select * from {{ ref('cf_hypertension_admission_4_month') }}
union all
select * from {{ ref('cf_hypoglycemia') }}
union all
select * from {{ ref('cf_hypoxic_ischemic_encephalopathy_two_year') }}
union all
select * from {{ ref('cf_immune_suppression') }}
union all
select * from {{ ref('cf_immune_suppression_diagnosis') }}
union all
select * from {{ ref('cf_immune_suppression_meds') }}
union all
select * from {{ ref('cf_inhalant_dependence_not_in_remission') }}
union all
select * from {{ ref('cf_ip_bh_admits_one') }}
union all
select * from {{ ref('cf_ip_bh_admits_two') }}
union all
select * from {{ ref('cf_ivd') }}
union all
select * from {{ ref('cf_ivd_current') }}
union all
select * from {{ ref('cf_ivd_prior') }}
union all
select * from {{ ref('cf_ivf') }}
union all
select * from {{ ref('cf_kidney_transplant') }}
union all
select * from {{ ref('cf_lab_checked_coumadin_level') }}
union all
select * from {{ ref('cf_learning_disabilities_two_year') }}
union all
select * from {{ ref('cf_lives_in_group_home') }}
union all
select * from {{ ref('cf_lung_volume_reduction_ever') }}
union all
select * from {{ ref('cf_low_self_efficacy') }}
union all
select * from {{ ref('cf_low_social_support') }}
union all
select * from {{ ref('cf_major_depressive_hcc_two_year') }}
union all
select * from {{ ref('cf_medfill_depression') }}
union all
select * from {{ ref('cf_medfill_diabetes') }}
union all
select * from {{ ref('cf_medfill_high_mod_statin_cvd') }}
union all
select * from {{ ref('cf_medfill_hypertension') }}
union all
select * from {{ ref('cf_medfill_statin_diabetes') }}
union all
select * from {{ ref('cf_medicaid_pregnancy_eligibility') }}
union all
select * from {{ ref('cf_medication_assisted_treatment_otp') }}
union all
select * from {{ ref('cf_medication_assisted_treatment_otp_4_months') }}
union all
select * from {{ ref('cf_medication_assisted_treatment_obot') }}
union all
select * from {{ ref('cf_medication_assisted_treatment_obot_4_months') }}
union all
select * from {{ ref('cf_metabolic_disorders_two_year') }}
union all
select * from {{ ref('cf_metabolic_hcc') }}
union all
select * from {{ ref('cf_muscular_pain_and_disease') }}
union all
select * from {{ ref('cf_mi') }}
union all
select * from {{ ref('cf_mood_disorders') }}
union all
select * from {{ ref('cf_mood_stabilizers') }}
union all
select * from {{ ref('cf_mood_stabilizers_not_seizure_medication') }}
union all
select * from {{ ref('cf_morbid_obesity') }}
union all
select * from {{ ref('cf_morbid_obesity_hcc') }}
union all
select * from {{ ref('cf_morbid_obesity_dx') }}
union all
select * from {{ ref('cf_morbid_obesity_hcc_two_year') }}
union all
select * from {{ ref('cf_multiple_sclerosis_hcc_two_year') }}
union all
select * from {{ ref('cf_muscular_dystrophy') }}
union all
select * from {{ ref('cf_needs_a_1_c') }}
union all
select * from {{ ref('cf_needs_bp') }}
union all
select * from {{ ref('cf_needs_bp_4_month') }}
union all
select * from {{ ref('cf_nephropathy_treatment') }}
union all
select * from {{ ref('cf_neonatal_abstinence_syndrome_two_year') }}
union all
select * from {{ ref('cf_neurologic_neuromuscular') }}
union all
select * from {{ ref('cf_noninfective_enteritis_colitis_two_year') }}
union all
select * from {{ ref('cf_obesity') }}
union all
select * from {{ ref('cf_on_chronic_opioids') }}
union all
select * from {{ ref('cf_on_coumadin') }}
union all
select * from {{ ref('cf_on_diabetes_medications') }}
union all
select * from {{ ref('cf_on_dialysis') }}
union all
select * from {{ ref('cf_on_high_mod_statin') }}
union all
select * from {{ ref('cf_on_inhaler') }}
union all
select * from {{ ref('cf_on_insulin') }}
union all
select * from {{ ref('cf_on_psychiatric_medication') }}
union all
select * from {{ ref('cf_on_psychiatric_medication_high_strength') }}
union all
select * from {{ ref('cf_on_psychiatric_medication_smi') }}
union all
select * from {{ ref('cf_on_statin') }}
union all
select * from {{ ref('cf_opioid_dependence_not_in_remission') }}
union all
select * from {{ ref('cf_one_pediatric_ccm_clinical_and_one_encounter_condition') }}
union all
select * from {{ ref('cf_osteoporosis') }}
union all
select * from {{ ref('cf_other_psychoactive_substance_dependence_not_in_remission') }}
union all
select * from {{ ref('cf_other_psychosis') }}
union all
select * from {{ ref('cf_other_revascularization') }}
union all
select * from {{ ref('cf_other_stimulant_dependence_not_in_remission') }}
union all
select * from {{ ref('cf_oxygen') }}
union all
select * from {{ ref('cf_paid_amount_twelve_thousand_acute') }}
union all
select * from {{ ref('cf_paid_amount_eighteen_thousand_medical') }}
union all
select * from {{ ref('cf_palliative') }}
union all
select * from {{ ref('cf_palliative_question') }}
union all
select * from {{ ref('cf_palliative_two_points') }}
union all
select * from {{ ref('cf_palliative_first_point') }}
union all
select * from {{ ref('cf_palliative_second_point') }}
union all
select * from {{ ref('cf_palliative_third_point') }}
union all
select * from {{ ref('cf_parkinsons') }}
union all
select * from {{ ref('cf_parkinsons_or_huntingtons_hcc_two_year') }}
union all
select * from {{ ref('cf_pci') }}
union all
select * from {{ ref('cf_pediatric_ccm_clinical_condition') }}
union all
select * from {{ ref('cf_pediatric_ccm_encounter_condition') }}
union all
select * from {{ ref('cf_pediatric_ccm_one_clinical_one_encounter_or_two_encounter_condition_or_assessment') }}
union all
select * from {{ ref('cf_polypharmacy') }}
union all
select * from {{ ref('cf_polypharmacy_chronic') }}
union all
select * from {{ ref('cf_polypharmacy_pediatric') }}
union all
select * from {{ ref('cf_post_organ_transplant_hcc_two_year') }}
union all
select * from {{ ref('cf_pressure_ulcer_stage_three_four_hcc_two_year') }}
union all
select * from {{ ref('cf_pulmonary_diagnosis') }}
union all
select * from {{ ref('cf_pulmonary_fibrosis_ever') }}
union all
select * from {{ ref('cf_pre_diabetes_dx') }}
union all
select * from {{ ref('cf_pregnancy') }}
union all
select * from {{ ref('cf_pregnancy_9_mo') }}
union all
select * from {{ ref('cf_pregnancy_9_mo_or_assessment') }}
union all
select * from {{ ref('cf_pregnancy_9_mo_or_assessment_under_55_and_female') }}
union all
select * from {{ ref('cf_pregnancy_assessment') }}
union all
select * from {{ ref('cf_premature_newborn_two_year') }}
union all
select * from {{ ref('cf_ptsd') }}
union all
select * from {{ ref('cf_quadriplegia') }}
union all
select * from {{ ref('cf_qualifies_for_financial_assistance_program') }}
union all
select * from {{ ref('cf_recently_discontinued_substance_use_treatments') }}
union all
select * from {{ ref('cf_recent_inpatient_mh') }}
union all
select * from {{ ref('cf_recent_outpatient_mh') }}
union all
select * from {{ ref('cf_renal_disease') }}
union all
select * from {{ ref('cf_respirator_dependence') }}
union all
select * from {{ ref('cf_respirator_dependence_outpatient') }}
union all
select * from {{ ref('cf_respiratory_failure_hcc_two_year') }}
union all
select * from {{ ref('cf_rx_opioids_multiple_providers') }}
union all
select * from {{ ref('cf_severe_allergy_w_anaphylaxis') }}
union all
select * from {{ ref('cf_schizophrenia') }}
union all
select * from {{ ref('cf_schizophrenia_hcc_two_year') }}
union all
select * from {{ ref('cf_sedative_hypnotic_anxiolytic_dependence_not_in_remission') }}
union all
select * from {{ ref('cf_seizure_disorder') }}
union all
select * from {{ ref('cf_seizure_medication') }}
union all
select * from {{ ref('cf_seizure_medication_not_seizure_disorder') }}
union all
select * from {{ ref('cf_serious_mental_illness') }}
union all
select * from {{ ref('cf_severe_head_injury_hcc_two_year') }}
union all
select * from {{ ref('cf_sickle_cell_disease_two_year') }}
union all
select * from {{ ref('cf_smoker') }}
union all
select * from {{ ref('cf_smoker_assessment') }}
union all
select * from {{ ref('cf_smoker_diagnosis') }}
union all
select * from {{ ref('cf_social_vulnerability') }}
union all
select * from {{ ref('cf_specified_heart_arrhythmias_hcc_two_year') }}
union all
select * from {{ ref('cf_spinal_cord_disorders_hcc_two_years') }}
union all
select * from {{ ref('cf_stroke') }}
union all
select * from {{ ref('cf_stroke_ever') }}
union all
select * from {{ ref('cf_stroke_hcc_two_year') }}
union all
select * from {{ ref('cf_substance_abuse_assessment') }}
union all
select * from {{ ref('cf_substance_abuse_hcc_two_year') }}
union all
select * from {{ ref('cf_substance_use_disorder_treatments') }}
union all
select * from {{ ref('cf_substance_use_disorder_treatments_4_months') }}
union all
select * from {{ ref('cf_substance_assessment_scores') }}
union all
select * from {{ ref('cf_substance_use_disorder') }}
union all
select * from {{ ref('cf_substance_use_disorder_counseling') }}
union all
select * from {{ ref('cf_substance_use_disorder_counseling_4_months') }}
union all
select * from {{ ref('cf_substance_use_other_treatments') }}
union all
select * from {{ ref('cf_substance_use_other_treatments_4_months') }}
union all
select * from {{ ref('cf_substance_use_disorder_rehab') }}
union all
select * from {{ ref('cf_substance_use_disorder_rehab_4_months') }}
union all
select * from {{ ref('cf_substance_use_disorder_residential') }}
union all
select * from {{ ref('cf_substance_use_disorder_residential_4_months') }}
union all
select * from {{ ref('cf_super_utilizer') }}
union all
select * from {{ ref('cf_super_utilizer_ct') }}
union all
select * from {{ ref('cf_super_utilizer_default') }}
union all
select * from {{ ref('cf_suspect_alcohol') }}
union all
select * from {{ ref('cf_suspect_depression') }}
union all
select * from {{ ref('cf_suspect_diabetes') }}
union all
select * from {{ ref('cf_suspect_diabetes_screening') }}
union all
select * from {{ ref('cf_suspect_drug_abuse') }}
union all
select * from {{ ref('cf_suspect_morbid_obesity') }}
union all
select * from {{ ref('cf_tracheostomy_status') }}
union all
select * from {{ ref('cf_transportation_challenges') }}
union all
select * from {{ ref('cf_traumatic_brain_injury') }}
union all
select * from {{ ref('cf_two_ed_visits') }}
union all
select * from {{ ref('cf_two_pediatric_ccm_encounter_condition') }}
union all
select * from {{ ref('cf_two_plus_adls') }}
union all
select * from {{ ref('cf_three_ed_visits') }}
union all
select * from {{ ref('cf_ulcers') }}
union all
select * from {{ ref('cf_unplanned_admit') }}
union all
select * from {{ ref('cf_unsafe_or_unstable_housing') }}
union all
select * from {{ ref('cf_unsafe_or_unstable_housing_one_year') }}
union all
select * from {{ ref('cf_unstable_housing_assessment_one_year') }}
union all
select * from {{ ref('cf_urine_protein_tests') }}
union all
select * from {{ ref('cf_vascular_disease_hcc_two_year') }}
union all
select * from {{ ref('cf_viral_hepatitis') }}
