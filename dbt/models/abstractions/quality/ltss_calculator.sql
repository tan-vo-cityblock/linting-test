with startingdata as (

SELECT
partnerName,
concat(patientName," ",derived_member.patientId) as patientNameAndID,
derived_member.patientId ,
derived_member.patientName,
derived_member.cohortName,
derived_member.latestRatingCategory,
questions_answers.questionSlug,
STRING_AGG(DISTINCT CAST(questions_answers.answerText  AS STRING), '|RECORD|') AS questions_answers_answer_text

FROM
{{ref('questions_answers_all')}} questions_answers

LEFT JOIN
{{ ref('member') }} derived_member
ON questions_answers.patientId=derived_member.patientId

LEFT JOIN
{{ ref('member_current_care_team_all') }} member_current_care_team_all
ON derived_member.patientId=member_current_care_team_all.patientId

WHERE
partnerName = 'Tufts'
and (questions_answers.assessmentSlug  = 'health-risk-assessment-ma' or questions_answers.assessmentSlug  ='minimum-data-set')
AND (CASE WHEN questions_answers.patientQuestionAnswerDayReverseIteration = 1 THEN TRUE WHEN questions_answers.patientQuestionAnswerDayReverseIteration > 1 THEN FALSE ELSE FALSE END)
AND questions_answers.isLatestPatientAnswer  = TRUE

GROUP BY
1, 2, 3, 4, 5, 6, 7
),


transposed as (

SELECT distinct
patientId,
patientName,
cohortName,
latestRatingCategory,
max(IF(questionSlug ='does-the-member-have-any-mental-illness-mds', trim(questions_answers_answer_text), null)) as does_the_member_have_any_mental_illness_mds,
max(IF(questionSlug ='specify-mental-illness', trim(questions_answers_answer_text), null)) as specify_mental_illness,
max(IF(questionSlug ='developmental-disability-prior-to-22', trim(questions_answers_answer_text), null)) as developmental_disability_prior_to_22,
max(IF(questionSlug ='community-services-recommended', trim(questions_answers_answer_text), null)) as community_services_recommended,
max(IF(questionSlug ='if-other-what-community-services-needed-question', trim(questions_answers_answer_text), null)) as if_other_what_community_services_needed_question,
max(IF(questionSlug ='is-home-or-apartment-available-for-member', trim(questions_answers_answer_text), null)) as is_home_or_apartment_available_for_member,
max(IF(questionSlug ='caregiver-to-assist-member-in-community', trim(questions_answers_answer_text), null)) as caregiver_to_assist_member_in_community,
max(IF(questionSlug ='unexplained-weight-gain', trim(questions_answers_answer_text), null)) as unexplained_weight_gain,
max(IF(questionSlug ='personal-care-services', trim(questions_answers_answer_text), null)) as personal_care_services,
max(IF(questionSlug ='days-per-week-personal-care', trim(questions_answers_answer_text), null)) as days_per_week_personal_care,
max(IF(questionSlug ='hours-per-week-personal-care', trim(questions_answers_answer_text), null)) as hours_per_week_personal_care,
max(IF(questionSlug ='significant-change-in-condition', trim(questions_answers_answer_text), null)) as significant_change_in_condition,
max(IF(questionSlug ='improvement-or-deterioration', trim(questions_answers_answer_text), null)) as improvement_or_deterioration,
max(IF(questionSlug ='indicate-changes-in-condition', trim(questions_answers_answer_text), null)) as indicate_changes_in_condition,
max(IF(questionSlug ='name-of-rn', trim(questions_answers_answer_text), null)) as name_of_rn,
max(IF(questionSlug ='highest-level-of-education-completed', questions_answers_answer_text, null)) as highest_level_of_education_completed,
max(IF(questionSlug ='client-has-legal-guardian', trim(questions_answers_answer_text), null)) as client_has_legal_guardian,
max(IF(questionSlug ='client-has-advanced-medical-directives', trim(questions_answers_answer_text), null)) as client_has_advanced_medical_directives,
max(IF(questionSlug ='date-time-case-opened', trim(questions_answers_answer_text), null)) as date_time_case_opened,
max(IF(questionSlug ='reason-for-referral', trim(questions_answers_answer_text), null)) as reason_for_referral,
max(IF(questionSlug ='skilled-nursing-treatments', trim(questions_answers_answer_text), null)) as skilled_nursing_treatments,
max(IF(questionSlug ='monitoring-to-avoid-clinical-complications', trim(questions_answers_answer_text), null)) as monitoring_to_avoid_clinical_complications,
max(IF(questionSlug ='rehabilitation', trim(questions_answers_answer_text), null)) as rehabilitation,
max(IF(questionSlug ='client-family-education', trim(questions_answers_answer_text), null)) as client_family_education,
max(IF(questionSlug ='family-respite', trim(questions_answers_answer_text), null)) as family_respite,
max(IF(questionSlug ='palliative-care', trim(questions_answers_answer_text), null)) as palliative_care,
max(IF(questionSlug ='time-since-last-discharge-from-inpatient-setting', trim(questions_answers_answer_text), null)) as time_since_last_discharge_from_inpatient_setting,
max(IF(questionSlug ='where-lived-at-time-of-referral', trim(questions_answers_answer_text), null)) as where_lived_at_time_of_referral,
max(IF(questionSlug ='who-lived-with-at-time-of-referral', trim(questions_answers_answer_text), null)) as who_lived_with_at_time_of_referral,
max(IF(questionSlug ='resided-in-nursing-home-during-past-5-years', trim(questions_answers_answer_text), null)) as resided_in_nursing_home_during_past_5_years,
max(IF(questionSlug ='moved-to-current-residence-in-past-two-years', trim(questions_answers_answer_text), null)) as moved_to_current_residence_in_past_two_years,
max(IF(questionSlug ='assessment-reference-date-time', trim(questions_answers_answer_text), null)) as assessment_reference_date_time,
max(IF(questionSlug ='reasons-for-assessment', trim(questions_answers_answer_text), null)) as reasons_for_assessment,
max(IF(questionSlug ='short-term-memory-question', trim(questions_answers_answer_text), null)) as short_term_memory_question,
max(IF(questionSlug ='procedural-memory-question', trim(questions_answers_answer_text), null)) as procedural_memory_question,
max(IF(questionSlug ='how-well-client-made-decision-about-organizing-day', trim(questions_answers_answer_text), null)) as how_well_client_made_decision_about_organizing_day,
max(IF(questionSlug ='worsening-of-decision-making', trim(questions_answers_answer_text), null)) as worsening_of_decision_making,
max(IF(questionSlug ='sudden-or-new-onset-change-in-mental-function', trim(questions_answers_answer_text), null)) as sudden_or_new_onset_change_in_mental_function,
max(IF(questionSlug ='client-agitated-or-disoriented-last-90-days', trim(questions_answers_answer_text), null)) as client_agitated_or_disoriented_last_90_days,
max(IF(questionSlug ='hearing', trim(questions_answers_answer_text), null)) as hearing,
max(IF(questionSlug ='making-self-understood', trim(questions_answers_answer_text), null)) as making_self_understood,
max(IF(questionSlug ='ability-to-understand-others', trim(questions_answers_answer_text), null)) as ability_to_understand_others,
max(IF(questionSlug ='worsening-communication', trim(questions_answers_answer_text), null)) as worsening_communication,
max(IF(questionSlug ='vision', trim(questions_answers_answer_text), null)) as vision,
max(IF(questionSlug ='sees-halos-vision', trim(questions_answers_answer_text), null)) as sees_halos_vision,
max(IF(questionSlug ='worsening-vision', trim(questions_answers_answer_text), null)) as worsening_vision,
max(IF(questionSlug ='feeling-of-sadness', trim(questions_answers_answer_text), null)) as feeling_of_sadness,
max(IF(questionSlug ='persistent-anger-at-self-or-others', trim(questions_answers_answer_text), null)) as persistent_anger_at_self_or_others,
max(IF(questionSlug ='expressions-of-what-appear-to-be-unrealistic-fears', trim(questions_answers_answer_text), null)) as expressions_of_what_appear_to_be_unrealistic_fears,
max(IF(questionSlug ='repetitive-health-complaints', trim(questions_answers_answer_text), null)) as repetitive_health_complaints,
max(IF(questionSlug ='repetitive-anxious-complaints', trim(questions_answers_answer_text), null)) as repetitive_anxious_complaints,
max(IF(questionSlug ='sad-pained-worried-facial-expressions', trim(questions_answers_answer_text), null)) as sad_pained_worried_facial_expressions,
max(IF(questionSlug ='recurrent-crying-tearfulness', trim(questions_answers_answer_text), null)) as recurrent_crying_tearfulness,
max(IF(questionSlug ='withdrawl-from-activities-of-interest', trim(questions_answers_answer_text), null)) as withdrawl_from_activities_of_interest,
max(IF(questionSlug ='reduced-social-interaction', trim(questions_answers_answer_text), null)) as reduced_social_interaction,
max(IF(questionSlug ='mood-indicators-became-worse', trim(questions_answers_answer_text), null)) as mood_indicators_became_worse,
max(IF(questionSlug ='wandering', trim(questions_answers_answer_text), null)) as wandering,
max(IF(questionSlug ='verbally-abusive-behavioral-symptoms', trim(questions_answers_answer_text), null)) as verbally_abusive_behavioral_symptoms,
max(IF(questionSlug ='physically-abusive-behavioral-symptoms', trim(questions_answers_answer_text), null)) as physically_abusive_behavioral_symptoms,
max(IF(questionSlug ='socially-inappropriate-behavioral-symptoms', trim(questions_answers_answer_text), null)) as socially_inappropriate_behavioral_symptoms,
max(IF(questionSlug ='resists-care', questions_answers_answer_text, null)) as resists_care,
max(IF(questionSlug ='behavioral-symptoms-worse-over-last-90-days', trim(questions_answers_answer_text), null)) as behavioral_symptoms_worse_over_last_90_days,
max(IF(questionSlug ='at-ease-interacting-with-others', trim(questions_answers_answer_text), null)) as at_ease_interacting_with_others,
max(IF(questionSlug ='openly-expresses-conflict', trim(questions_answers_answer_text), null)) as openly_expresses_conflict,
max(IF(questionSlug ='if-decline-was-client-distressed', trim(questions_answers_answer_text), null)) as if_decline_was_client_distressed,
max(IF(questionSlug ='length-of-time-alone-during-day', trim(questions_answers_answer_text), null)) as length_of_time_alone_during_day,
max(IF(questionSlug ='client-is-lonely', trim(questions_answers_answer_text), null)) as client_is_lonely,
max(IF(questionSlug ='does-member-have-key-informal-helper-primary', trim(questions_answers_answer_text), null)) as does_member_have_key_informal_helper_primary,
max(IF(questionSlug ='full-name-key-informal-helper', trim(questions_answers_answer_text), null)) as full_name_key_informal_helper,
max(IF(questionSlug ='lives-with-client-key-informal-helper', trim(questions_answers_answer_text), null)) as lives_with_client_key_informal_helper,
max(IF(questionSlug ='relationship-to-client-key-informal-helper', trim(questions_answers_answer_text), null)) as relationship_to_client_key_informal_helper,
max(IF(questionSlug ='advice-or-emotional-support-areas-of-help', trim(questions_answers_answer_text), null)) as advice_or_emotional_support_areas_of_help,
max(IF(questionSlug ='iadl-care-areas-of-help', trim(questions_answers_answer_text), null)) as iadl_care_areas_of_help,
max(IF(questionSlug ='adl-care-areas-of-help', trim(questions_answers_answer_text), null)) as adl_care_areas_of_help,
max(IF(questionSlug ='advice-or-emotional-support-willingness-to-increase-help', trim(questions_answers_answer_text), null)) as advice_or_emotional_support_willingness_to_increase_help,
max(IF(questionSlug ='adl-care-willingness-to-increase-help', trim(questions_answers_answer_text), null)) as adl_care_willingness_to_increase_help,
max(IF(questionSlug ='iadl-care-willingness-to-increase-help', trim(questions_answers_answer_text), null)) as iadl_care_willingness_to_increase_help,
max(IF(questionSlug ='does-member-have-key-informal-helper-secondary', trim(questions_answers_answer_text), null)) as does_member_have_key_informal_helper_secondary,
max(IF(questionSlug ='full-name-secondary-key-informal-helper', trim(questions_answers_answer_text), null)) as full_name_secondary_key_informal_helper,
max(IF(questionSlug ='lives-with-client-secondary-key-informal-helper', trim(questions_answers_answer_text), null)) as lives_with_client_secondary_key_informal_helper,
max(IF(questionSlug ='relationship-to-client-secondary-key-informal-helper', trim(questions_answers_answer_text), null)) as relationship_to_client_secondary_key_informal_helper,
max(IF(questionSlug ='advice-or-emotional-support-secondary-areas-of-help', trim(questions_answers_answer_text), null)) as advice_or_emotional_support_secondary_areas_of_help,
max(IF(questionSlug ='iadl-care-secondary-areas-of-help', trim(questions_answers_answer_text), null)) as iadl_care_secondary_areas_of_help,
max(IF(questionSlug ='adl-care-secondary-areas-of-help', trim(questions_answers_answer_text), null)) as adl_care_secondary_areas_of_help,
max(IF(questionSlug ='advice-or-emotional-support-secondary-willingness-to-increase-help', trim(questions_answers_answer_text), null)) as advice_or_emotional_support_secondary_willingness_to_increase_help,
max(IF(questionSlug ='adl-care-secondary-willingness-to-increase-help', trim(questions_answers_answer_text), null)) as adl_care_secondary_willingness_to_increase_help,
max(IF(questionSlug ='caregiver-status', trim(questions_answers_answer_text), null)) as caregiver_status,
max(IF(questionSlug ='sum-of-time-across-five-weekdays', trim(questions_answers_answer_text), null)) as sum_of_time_across_five_weekdays,
max(IF(questionSlug ='sum-of-time-across-two-weekend-days', trim(questions_answers_answer_text), null)) as sum_of_time_across_two_weekend_days,
max(IF(questionSlug ='meal-preparation-self-performance-code', trim(questions_answers_answer_text), null)) as meal_preparation_self_performance_code,
max(IF(questionSlug ='meal-preparation-difficulty-code', trim(questions_answers_answer_text), null)) as meal_preparation_difficulty_code,
max(IF(questionSlug ='ordinary-housework-self-performance-code', trim(questions_answers_answer_text), null)) as ordinary_housework_self_performance_code,
max(IF(questionSlug ='ordinary-housework-difficulty-code', trim(questions_answers_answer_text), null)) as ordinary_housework_difficulty_code,
max(IF(questionSlug ='managing-finance-self-performance-code', trim(questions_answers_answer_text), null)) as managing_finance_self_performance_code,
max(IF(questionSlug ='managing-finance-difficulty-code', trim(questions_answers_answer_text), null)) as managing_finance_difficulty_code,
max(IF(questionSlug ='managing-medications-self-performance-code', trim(questions_answers_answer_text), null)) as managing_medications_self_performance_code,
max(IF(questionSlug ='managing-medications-difficulty-code', trim(questions_answers_answer_text), null)) as managing_medications_difficulty_code,
max(IF(questionSlug ='phone-use-self-performance-code', trim(questions_answers_answer_text), null)) as phone_use_self_performance_code,
max(IF(questionSlug ='phone-use-difficulty-code', trim(questions_answers_answer_text), null)) as phone_use_difficulty_code,
max(IF(questionSlug ='shopping-self-performance-code', trim(questions_answers_answer_text), null)) as shopping_self_performance_code,
max(IF(questionSlug ='shopping-difficulty-code', trim(questions_answers_answer_text), null)) as shopping_difficulty_code,
max(IF(questionSlug ='transportation-self-performance-code', trim(questions_answers_answer_text), null)) as transportation_self_performance_code,
max(IF(questionSlug ='transportation-difficulty-code', trim(questions_answers_answer_text), null)) as transportation_difficulty_code,
max(IF(questionSlug ='mobility-in-bed-self-performance', trim(questions_answers_answer_text), null)) as mobility_in_bed_self_performance,
max(IF(questionSlug ='transfer-self-performance', trim(questions_answers_answer_text), null)) as transfer_self_performance,
max(IF(questionSlug ='locomotion-in-home-self-performance-code', trim(questions_answers_answer_text), null)) as locomotion_in_home_self_performance_code,
max(IF(questionSlug ='locomotion-outside-of-home-self-performance-code', trim(questions_answers_answer_text), null)) as locomotion_outside_of_home_self_performance_code,
max(IF(questionSlug ='dressing-upper-body-self-performance', trim(questions_answers_answer_text), null)) as dressing_upper_body_self_performance,
max(IF(questionSlug ='dressing-lower-body-self-performance', trim(questions_answers_answer_text), null)) as dressing_lower_body_self_performance,
max(IF(questionSlug ='eating-self-performance', trim(questions_answers_answer_text), null)) as eating_self_performance,
max(IF(questionSlug ='toilet-use-self-performance', trim(questions_answers_answer_text), null)) as toilet_use_self_performance,
max(IF(questionSlug ='personal-hygiene-self-performance', trim(questions_answers_answer_text), null)) as personal_hygiene_self_performance,
max(IF(questionSlug ='bathing-self-performance', trim(questions_answers_answer_text), null)) as bathing_self_performance,
max(IF(questionSlug ='adl-status-has-become-worse', trim(questions_answers_answer_text), null)) as adl_status_has_become_worse,
max(IF(questionSlug ='primary-modes-of-locomotion-indoors', trim(questions_answers_answer_text), null)) as primary_modes_of_locomotion_indoors,
max(IF(questionSlug ='primary-modes-of-locomotion-outdoors', trim(questions_answers_answer_text), null)) as primary_modes_of_locomotion_outdoors,
max(IF(questionSlug ='how-client-went-up-and-down-stairs', trim(questions_answers_answer_text), null)) as how_client_went_up_and_down_stairs,
max(IF(questionSlug ='number-of-days-client-went-out-of-the-house', trim(questions_answers_answer_text), null)) as number_of_days_client_went_out_of_the_house,
max(IF(questionSlug ='hours-of-physical-activities-in-the-last-3-days', trim(questions_answers_answer_text), null)) as hours_of_physical_activities_in_the_last_3_days,
max(IF(questionSlug ='functional-potential', trim(questions_answers_answer_text), null)) as functional_potential,
max(IF(questionSlug ='control-of-urinary-bladder-function', trim(questions_answers_answer_text), null)) as control_of_urinary_bladder_function,
max(IF(questionSlug ='worsening-of-bladder-control', trim(questions_answers_answer_text), null)) as worsening_of_bladder_control,
max(IF(questionSlug ='bladder-devices', trim(questions_answers_answer_text), null)) as bladder_devices,
max(IF(questionSlug ='control-of-bowel-movement', trim(questions_answers_answer_text), null)) as control_of_bowel_movement,
max(IF(questionSlug ='cerebrovascular-accident', trim(questions_answers_answer_text), null)) as cerebrovascular_accident,
max(IF(questionSlug ='congestive-heart-failure', trim(questions_answers_answer_text), null)) as congestive_heart_failure,
max(IF(questionSlug ='coronary-artery-disease', trim(questions_answers_answer_text), null)) as coronary_artery_disease,
max(IF(questionSlug ='hypertension', trim(questions_answers_answer_text), null)) as hypertension,
max(IF(questionSlug ='irregularly-irregular-pulse', trim(questions_answers_answer_text), null)) as irregularly_irregular_pulse,
max(IF(questionSlug ='peripheral-vascular-disease', trim(questions_answers_answer_text), null)) as peripheral_vascular_disease,
max(IF(questionSlug ='alzheiemers', trim(questions_answers_answer_text), null)) as alzheiemers,
max(IF(questionSlug ='dementia-other-than-alzheiemers-disease', trim(questions_answers_answer_text), null)) as dementia_other_than_alzheiemers_disease,
max(IF(questionSlug ='head-trauma', trim(questions_answers_answer_text), null)) as head_trauma,
max(IF(questionSlug ='hemiplegia-or-hemiparesis', trim(questions_answers_answer_text), null)) as hemiplegia_or_hemiparesis,
max(IF(questionSlug ='multiple-sclerosis', trim(questions_answers_answer_text), null)) as multiple_sclerosis,
max(IF(questionSlug ='parkinsonism', trim(questions_answers_answer_text), null)) as parkinsonism,
max(IF(questionSlug ='arthritis', trim(questions_answers_answer_text), null)) as arthritis,
max(IF(questionSlug ='hip-fracture', trim(questions_answers_answer_text), null)) as hip_fracture,
max(IF(questionSlug ='other-fractures', trim(questions_answers_answer_text), null)) as other_fractures,
max(IF(questionSlug ='osteoporosis', trim(questions_answers_answer_text), null)) as osteoporosis,
max(IF(questionSlug ='cataract', trim(questions_answers_answer_text), null)) as cataract,
max(IF(questionSlug ='glaucoma', trim(questions_answers_answer_text), null)) as glaucoma,
max(IF(questionSlug ='any-psychiatric-diagnosis', trim(questions_answers_answer_text), null)) as any_psychiatric_diagnosis,
max(IF(questionSlug ='hiv-infection', trim(questions_answers_answer_text), null)) as hiv_infection,
max(IF(questionSlug ='pneumonia', trim(questions_answers_answer_text), null)) as pneumonia,
max(IF(questionSlug ='tuberculosis', trim(questions_answers_answer_text), null)) as tuberculosis,
max(IF(questionSlug ='urinary-tract-infection', trim(questions_answers_answer_text), null)) as urinary_tract_infection,
max(IF(questionSlug ='cancer', trim(questions_answers_answer_text), null)) as cancer,
max(IF(questionSlug ='diabetes', trim(questions_answers_answer_text), null)) as diabetes,
max(IF(questionSlug ='emphysema-copd-asthma', trim(questions_answers_answer_text), null)) as emphysema_copd_asthma,
max(IF(questionSlug ='renal-failure', trim(questions_answers_answer_text), null)) as renal_failure,
max(IF(questionSlug ='thyroid-disease', trim(questions_answers_answer_text), null)) as thyroid_disease,
max(IF(questionSlug ='diagnoses-codes-descriptions', trim(questions_answers_answer_text), null)) as diagnoses_codes_descriptions,
max(IF(questionSlug ='preventive-health-past-two-years', trim(questions_answers_answer_text), null)) as preventive_health_past_two_years,
max(IF(questionSlug ='problem-conditions-present-on-2-or-more-days', trim(questions_answers_answer_text), null)) as problem_conditions_present_on_2_or_more_days,
max(IF(questionSlug ='problem-conditions-present-on-last-3-days', trim(questions_answers_answer_text), null)) as problem_conditions_present_on_last_3_days,
max(IF(questionSlug ='frequency-client-complains-of-pain', trim(questions_answers_answer_text), null)) as frequency_client_complains_of_pain,
max(IF(questionSlug ='intensity-of-pain', trim(questions_answers_answer_text), null)) as intensity_of_pain,
max(IF(questionSlug ='pain-intensity-disrupts-usual-activities', trim(questions_answers_answer_text), null)) as pain_intensity_disrupts_usual_activities,
max(IF(questionSlug ='character-of-pain', trim(questions_answers_answer_text), null)) as character_of_pain,
max(IF(questionSlug ='medications-adequately-control-pain', trim(questions_answers_answer_text), null)) as medications_adequately_control_pain,
max(IF(questionSlug ='unsteady-gait', trim(questions_answers_answer_text), null)) as unsteady_gait,
max(IF(questionSlug ='client-limits-going-outdoors-fear-of-falling', trim(questions_answers_answer_text), null)) as client_limits_going_outdoors_fear_of_falling,
max(IF(questionSlug ='cut-down-on-drinking', trim(questions_answers_answer_text), null)) as cut_down_on_drinking,
max(IF(questionSlug ='drink-first-thing-in-the-morning', trim(questions_answers_answer_text), null)) as drink_first_thing_in_the_morning,
max(IF(questionSlug ='chewed-tobacco-daily', trim(questions_answers_answer_text), null)) as chewed_tobacco_daily,
max(IF(questionSlug ='health-status-indicators', trim(questions_answers_answer_text), null)) as health_status_indicators,
max(IF(questionSlug ='other-status-indicators', trim(questions_answers_answer_text), null)) as other_status_indicators,
max(IF(questionSlug ='unintended-weight-loss', trim(questions_answers_answer_text), null)) as unintended_weight_loss,
max(IF(questionSlug ='severe-malnutrition', trim(questions_answers_answer_text), null)) as severe_malnutrition,
max(IF(questionSlug ='morbid-obesity', trim(questions_answers_answer_text), null)) as morbid_obesity,
max(IF(questionSlug ='ate-one-or-fewer-meals', trim(questions_answers_answer_text), null)) as ate_one_or_fewer_meals,
max(IF(questionSlug ='noticeable-decrease-in-food-last-3-days', trim(questions_answers_answer_text), null)) as noticeable_decrease_in_food_last_3_days,
max(IF(questionSlug ='insufficient-fluid', trim(questions_answers_answer_text), null)) as insufficient_fluid,
max(IF(questionSlug ='enteral-tube-feeding', trim(questions_answers_answer_text), null)) as enteral_tube_feeding,
max(IF(questionSlug ='swallowing', trim(questions_answers_answer_text), null)) as swallowing,
max(IF(questionSlug ='oral-status', trim(questions_answers_answer_text), null)) as oral_status,
max(IF(questionSlug ='troubling-skin-conditions', trim(questions_answers_answer_text), null)) as troubling_skin_conditions,
max(IF(questionSlug ='presence-of-pressure-ulcer', trim(questions_answers_answer_text), null)) as presence_of_pressure_ulcer,
max(IF(questionSlug ='presence-of-statis-ulcer', trim(questions_answers_answer_text), null)) as presence_of_statis_ulcer,
max(IF(questionSlug ='other-skin-problems-requiring-treatment', trim(questions_answers_answer_text), null)) as other_skin_problems_requiring_treatment,
max(IF(questionSlug ='client-previously-had-ulcer', trim(questions_answers_answer_text), null)) as client_previously_had_ulcer,
max(IF(questionSlug ='wound-or-ulcer-care', trim(questions_answers_answer_text), null)) as wound_or_ulcer_care,
max(IF(questionSlug ='home-environment', trim(questions_answers_answer_text), null)) as home_environment,
max(IF(questionSlug ='client-now-lives-with-other-persons', trim(questions_answers_answer_text), null)) as client_now_lives_with_other_persons,
max(IF(questionSlug ='client-better-in-other-living-environment', trim(questions_answers_answer_text), null)) as client_better_in_other_living_environment,
max(IF(questionSlug ='home-health-aide-days-hours-mins', trim(questions_answers_answer_text), null)) as home_health_aide_days_hours_mins,
max(IF(questionSlug ='visiting-nurse-days-hours-mins', trim(questions_answers_answer_text), null)) as visiting_nurse_days_hours_mins,
max(IF(questionSlug ='homemaking-services-days-hours-mins', trim(questions_answers_answer_text), null)) as homemaking_services_days_hours_mins,
max(IF(questionSlug ='meals-days-hours-mins', trim(questions_answers_answer_text), null)) as meals_days_hours_mins,
max(IF(questionSlug ='volunteer-services-days-hours-mins', trim(questions_answers_answer_text), null)) as volunteer_services_days_hours_mins,
max(IF(questionSlug ='physical-therapy-days-hours-mins', trim(questions_answers_answer_text), null)) as physical_therapy_days_hours_mins,
max(IF(questionSlug ='occupational-therapy-days-hours-mins', trim(questions_answers_answer_text), null)) as occupational_therapy_days_hours_mins,
max(IF(questionSlug ='speech-therapy-days-hours-mins', trim(trim(questions_answers_answer_text)), null)) as speech_therapy_days_hours_mins,
max(IF(questionSlug ='day-care-or-day-hospital-days-hours-mins', trim(questions_answers_answer_text), null)) as day_care_or_day_hospital_days_hours_mins,
max(IF(questionSlug ='social-worker-at-home-days-hours-mins', trim(questions_answers_answer_text), null)) as social_worker_at_home_days_hours_mins,
max(IF(questionSlug ='oxygen-respiratory-treatment', trim(questions_answers_answer_text), null)) as oxygen_respiratory_treatment,
max(IF(questionSlug ='respirator-for-assistive-breathing', trim(questions_answers_answer_text), null)) as respirator_for_assistive_breathing,
max(IF(questionSlug ='other-respiratory-treatments', trim(questions_answers_answer_text), null)) as other_respiratory_treatments,
max(IF(questionSlug ='alcohol-or-drug-treatment-program', trim(questions_answers_answer_text), null)) as alcohol_or_drug_treatment_program,
max(IF(questionSlug ='blood-transfusions-treatment', trim(questions_answers_answer_text), null)) as blood_transfusions_treatment,
max(IF(questionSlug ='chemotherapy-treatment', trim(questions_answers_answer_text), null)) as chemotherapy_treatment,
max(IF(questionSlug ='dialysis-treatment', trim(questions_answers_answer_text), null)) as dialysis_treatment,
max(IF(questionSlug ='iv-infusion-central-treatment', trim(questions_answers_answer_text), null)) as iv_infusion_central_treatment,
max(IF(questionSlug ='iv-infusion-peripheral-treatment', trim(questions_answers_answer_text), null)) as iv_infusion_peripheral_treatment,
max(IF(questionSlug ='medication-by-injection-treatment', trim(questions_answers_answer_text), null)) as medication_by_injection_treatment,
max(IF(questionSlug ='ostomy-care', trim(questions_answers_answer_text), null)) as ostomy_care,
max(IF(questionSlug ='radiation-treatment', trim(questions_answers_answer_text), null)) as radiation_treatment,
max(IF(questionSlug ='tracheotomy-care', trim(questions_answers_answer_text), null)) as tracheotomy_care,
max(IF(questionSlug ='exercise-therapy', trim(questions_answers_answer_text), null)) as exercise_therapy,
max(IF(questionSlug ='occupational-therapy', trim(questions_answers_answer_text), null)) as occupational_therapy,
max(IF(questionSlug ='physical-therapy', trim(questions_answers_answer_text), null)) as physical_therapy,
max(IF(questionSlug ='day-center-program', trim(questions_answers_answer_text), null)) as day_center_program,
max(IF(questionSlug ='day-hospital-program', trim(questions_answers_answer_text), null)) as day_hospital_program,
max(IF(questionSlug ='hospice-care', trim(questions_answers_answer_text), null)) as hospice_care,
max(IF(questionSlug ='physician-or-clinic-visit', trim(questions_answers_answer_text), null)) as physician_or_clinic_visit,
max(IF(questionSlug ='respite-care', trim(questions_answers_answer_text), null)) as respite_care,
max(IF(questionSlug ='daily-nurse-monitoring', trim(questions_answers_answer_text), null)) as daily_nurse_monitoring,
max(IF(questionSlug ='nurse-monitoring-less-than-daily', trim(questions_answers_answer_text), null)) as nurse_monitoring_less_than_daily,
max(IF(questionSlug ='medic-alert-bracelet', trim(questions_answers_answer_text), null)) as medic_alert_bracelet,
max(IF(questionSlug ='skin-treatment', trim(questions_answers_answer_text), null)) as skin_treatment,
max(IF(questionSlug ='special-diet', trim(questions_answers_answer_text), null)) as special_diet,
max(IF(questionSlug ='oxygen-equipment', trim(questions_answers_answer_text), null)) as oxygen_equipment,
max(IF(questionSlug ='iv-equipment', trim(questions_answers_answer_text), null)) as iv_equipment,
max(IF(questionSlug ='catheter-equipment', trim(questions_answers_answer_text), null)) as catheter_equipment,
max(IF(questionSlug ='ostomy-equipment', trim(questions_answers_answer_text), null)) as ostomy_equipment,
max(IF(questionSlug ='number-of-times-admitted-to-hospital', trim(questions_answers_answer_text), null)) as number_of_times_admitted_to_hospital,
max(IF(questionSlug ='number-of-times-visited-emergency-room', trim(questions_answers_answer_text), null)) as number_of_times_visited_emergency_room,
max(IF(questionSlug ='emergent-care', trim(questions_answers_answer_text), null)) as emergent_care,
max(IF(questionSlug ='treatment-goals-met', trim(questions_answers_answer_text), null)) as treatment_goals_met,
max(IF(questionSlug ='overall-self-sufficiency', trim(questions_answers_answer_text), null)) as overall_self_sufficiency,
max(IF(questionSlug ='limited-funds', trim(questions_answers_answer_text), null)) as limited_funds,
max(IF(questionSlug ='number-of-different-medicines', trim(questions_answers_answer_text), null)) as number_of_different_medicines,
max(IF(questionSlug ='antipsychotic-medication', trim(questions_answers_answer_text), null)) as antipsychotic_medication,
max(IF(questionSlug ='anxiolytic-medication', trim(questions_answers_answer_text), null)) as anxiolytic_medication,
max(IF(questionSlug ='antidepressant-medication', trim(questions_answers_answer_text), null)) as antidepressant_medication,
max(IF(questionSlug ='hypnotic-medication-question', trim(questions_answers_answer_text), null)) as hypnotic_medication_question,
max(IF(questionSlug ='physician-reviewed-client-medications', trim(questions_answers_answer_text), null)) as physician_reviewed_client_medications,
max(IF(questionSlug ='compliant-with-medications', trim(questions_answers_answer_text), null)) as compliant_with_medications,
max(IF(questionSlug ='name-and-dose', trim(questions_answers_answer_text), null)) as name_and_dose,
max(IF(questionSlug ='ready-for-review', trim(questions_answers_answer_text), null)) as ready_for_review,
max(IF(questionSlug ='has-the-gateway-review-been-completed', trim(questions_answers_answer_text), null)) as has_the_gateway_review_been_completed,
max(IF(questionSlug ='what-is-the-gateway-result', trim(questions_answers_answer_text), null)) as what_is_the_gateway_result

FROM
startingdata

GROUP BY
1,2,3,4
),




compiled as (

select
patientId,
patientName,
cohortName,
latestRatingCategory,
case when ready_for_review = 'Yes' then true
      end as member_ready_for_review,
assessment_reference_date_time,
name_of_rn as RN_Completing_MDS,


--mobility_in_bed
mobility_in_bed_self_performance,

case when mobility_in_bed_self_performance in ('Total dependence','Maximal assistance','Extensive assistance','Limited assistance') then 1 else 0 end as mobility_in_bed_self_performance_code,

Case when mobility_in_bed_self_performance = "Activity did not occur" then 0
when mobility_in_bed_self_performance = "Independent" then 0
when mobility_in_bed_self_performance = "Setup help only" then 14
when mobility_in_bed_self_performance = "Supervision" then 14
when mobility_in_bed_self_performance = "Limited assistance" then 28
when mobility_in_bed_self_performance = "Extensive assistance" then 56
when mobility_in_bed_self_performance = "Maximal assistance" then 98
when mobility_in_bed_self_performance = "Total dependence" then 140
      end as mobility_in_bed_minutes_week,


--transfer_self_performance
transfer_self_performance,

case when transfer_self_performance in ('Total dependence','Maximal assistance','Extensive assistance','Limited assistance') then 1 else 0
      end as transfer_self_performance_code,

case when transfer_self_performance = "Activity did not occur" then 0
when transfer_self_performance = "Independent" then 0
when transfer_self_performance = "Setup help only" then 35
when transfer_self_performance = "Supervision" then 35
when transfer_self_performance = "Limited assistance" then 70
when transfer_self_performance = "Extensive assistance" then 175
when transfer_self_performance = "Maximal assistance" then 245
when transfer_self_performance = "Total dependence" then 350
      end as transfer_self_performance_minutes_week,


--locomotion in home
locomotion_in_home_self_performance_code as locomotion_in_home_self_performance,

case when locomotion_in_home_self_performance_code in ('Total dependence','Maximal assistance','Extensive assistance','Limited assistance') then 1 else 0
      end as locomotion_in_home_self_performance_code,

case when locomotion_in_home_self_performance_code = "Activity did not occur" then 0
when locomotion_in_home_self_performance_code = "Independent" then 0
when locomotion_in_home_self_performance_code = "Setup help only" then 0
when locomotion_in_home_self_performance_code = "Supervision" then 0
when locomotion_in_home_self_performance_code = "Limited assistance" then 0
when locomotion_in_home_self_performance_code = "Extensive assistance" then 0
when locomotion_in_home_self_performance_code = "Maximal assistance" then 0
when locomotion_in_home_self_performance_code = "Total dependence" then 0
      end as locomotion_in_home_self_performance_week,


--locomotion outside of home
locomotion_outside_of_home_self_performance_code as locomotion_outside_of_home_self_performance,

case when locomotion_outside_of_home_self_performance_code in ('Total dependence','Maximal assistance','Extensive assistance','Limited assistance') then 1 else 0
      end as locomotion_outside_of_home_self_performance_code,

case when locomotion_outside_of_home_self_performance_code = "Activity did not occur" then 0
when locomotion_outside_of_home_self_performance_code = "Independent" then 0
when locomotion_outside_of_home_self_performance_code = "Setup help only" then 0
when locomotion_outside_of_home_self_performance_code = "Supervision" then 1
when locomotion_outside_of_home_self_performance_code = "Limited assistance" then 5
when locomotion_outside_of_home_self_performance_code = "Extensive assistance" then 10
when locomotion_outside_of_home_self_performance_code = "Maximal assistance" then 20
when locomotion_outside_of_home_self_performance_code = "Total dependence" then 30
     end as locomotion_outside_of_home_self_performance_week,


--dressing upper body
dressing_upper_body_self_performance,

case when dressing_upper_body_self_performance in ('Total dependence','Maximal assistance','Extensive assistance','Limited assistance') then 1 else 0
      end as dressing_upper_body_self_performance_code,

case when dressing_upper_body_self_performance = "Activity did not occur" then 0
when dressing_upper_body_self_performance = "Independent" then 0
when dressing_upper_body_self_performance = "Setup help only" then 7
when dressing_upper_body_self_performance = "Supervision" then 21
when dressing_upper_body_self_performance = "Limited assistance" then 35
when dressing_upper_body_self_performance = "Extensive assistance" then 49
when dressing_upper_body_self_performance = "Maximal assistance" then 70
when dressing_upper_body_self_performance = "Total dependence" then 105
      end as dressing_upper_body_self_performance_week,


--dressing lower body
dressing_lower_body_self_performance,
case when dressing_lower_body_self_performance in ('Total dependence','Maximal assistance','Extensive assistance','Limited assistance') then 1 else 0
      end as dressing_lower_body_self_performance_code,

case when dressing_lower_body_self_performance = "Activity did not occur" then 0
when dressing_lower_body_self_performance = "Independent" then 0
when dressing_lower_body_self_performance = "Setup help only" then 7
when dressing_lower_body_self_performance = "Supervision" then 28
when dressing_lower_body_self_performance = "Limited assistance" then 49
when dressing_lower_body_self_performance = "Extensive assistance" then 70
when dressing_lower_body_self_performance = "Maximal assistance" then 105
when dressing_lower_body_self_performance = "Total dependence" then 140
      end as dressing_lower_body_self_performance_week,


--eating self performance
eating_self_performance,

case when eating_self_performance in ('Total dependence','Maximal assistance','Extensive assistance','Limited assistance') then 1 else 0
      end as eating_self_performance_code,

case when eating_self_performance = "Activity did not occur" then 0
when eating_self_performance = "Independent" then 0
when eating_self_performance = "Setup help only" then 21
when eating_self_performance = "Supervision" then 42
when eating_self_performance = "Limited assistance" then 63
when eating_self_performance = "Extensive assistance" then 105
when eating_self_performance = "Maximal assistance" then 147
when eating_self_performance = "Total dependence" then 210
      end as eating_self_performance_week,


--toilet use performance
toilet_use_self_performance,

case when toilet_use_self_performance in ('Total dependence','Maximal assistance','Extensive assistance','Limited assistance') then 1 else 0
      end as toilet_use_self_performance_code,

case when toilet_use_self_performance = "Activity did not occur" then 0
when toilet_use_self_performance = "Independent" then 0
when toilet_use_self_performance = "Setup help only" then 42
when toilet_use_self_performance = "Supervision" then 84
when toilet_use_self_performance = "Limited assistance" then 210
when toilet_use_self_performance = "Extensive assistance" then 336
when toilet_use_self_performance = "Maximal assistance" then 336
when toilet_use_self_performance = "Total dependence" then 504
      end as toilet_use_self_performance_week,


--personal hygiene performance
personal_hygiene_self_performance,

case when personal_hygiene_self_performance in ('Total dependence','Maximal assistance','Extensive assistance','Limited assistance') then 1 else 0
      end as personal_hygiene_self_performance_code,

case when personal_hygiene_self_performance = "Activity did not occur" then 0
when personal_hygiene_self_performance = "Independent" then 0
when personal_hygiene_self_performance = "Setup help only" then 14
when personal_hygiene_self_performance = "Supervision" then 14
when personal_hygiene_self_performance = "Limited assistance" then 35
when personal_hygiene_self_performance = "Extensive assistance" then 49
when personal_hygiene_self_performance = "Maximal assistance" then 70
when personal_hygiene_self_performance = "Total dependence" then 105
      end as personal_hygiene_self_performance_week,


--bathing self performance
bathing_self_performance,

case when bathing_self_performance in ('Total dependence','Maximal assistance','Extensive assistance','Limited assistance') then 1 else 0
      end as bathing_self_performance_code,

case when bathing_self_performance = "Activity did not occur" then 0
when bathing_self_performance = "Independent" then 0
when bathing_self_performance = "Setup help only" then 14
when bathing_self_performance = "Supervision" then 14
when bathing_self_performance = "Limited assistance" then 140
when bathing_self_performance = "Extensive assistance" then 175
when bathing_self_performance = "Maximal assistance" then 210
when bathing_self_performance = "Total dependence" then 210
      end as bathing_self_performance_week,


--meal prep self performance
meal_preparation_self_performance_code as meal_preparation_self_performance,

case when meal_preparation_self_performance_code in ('By others','Full help','Some help - help some of the time') then 1 else 0
      end as meal_preparation_self_performance_code,

case when meal_preparation_self_performance_code = "Activity did not occur" then 0
when meal_preparation_self_performance_code = "Independent" then 0
when meal_preparation_self_performance_code = "Some help - help some of the time" then 130
when meal_preparation_self_performance_code = "Full help" then 357
when meal_preparation_self_performance_code = "By others" then 420
      end as meal_preparation_self_performance_weekly,


--houseowork performance
ordinary_housework_self_performance_code as ordinary_housework_self_performance,

case when ordinary_housework_self_performance_code in ('By others','Full help','Some help - help some of the time') then 1 else 0 end as ordinary_housework_self_performance_code,

case when ordinary_housework_self_performance_code = "Activity did not occur" then 0
when ordinary_housework_self_performance_code = "Independent" then 0
when ordinary_housework_self_performance_code = "Some help - help some of the time" then 180
when ordinary_housework_self_performance_code = "Full help" then 180
when ordinary_housework_self_performance_code = "By others" then 180
    end as ordinary_housework_self_performance_weekly,


--finance performance
managing_finance_self_performance_code as managing_finance_self_performance,

case when managing_finance_self_performance_code in ('By others','Full help','Some help - help some of the time') then 1 else 0 end as managing_finance_self_performance_code,

case when managing_finance_self_performance_code = "Activity did not occur" then 0
when managing_finance_self_performance_code = "Independent" then 0
when managing_finance_self_performance_code = "Some help - help some of the time" then 0
when managing_finance_self_performance_code = "Full help" then 0
when managing_finance_self_performance_code = "By others" then 0
      end as managing_finance_self_performance_weekly,


--medication performance
managing_medications_self_performance_code as managing_medications_self_performance,

case when managing_medications_self_performance_code in ('By others','Full help','Some help - help some of the time') then 1 else 0
      end as managing_medications_self_performance_code,

case when managing_medications_self_performance_code = "Activity did not occur" then 0
when managing_medications_self_performance_code = "Independent" then 0
when managing_medications_self_performance_code = "Some help - help some of the time" then 20
when managing_medications_self_performance_code = "Full help" then 20
when managing_medications_self_performance_code = "By others" then 20
     end as managing_medications_self_performance_weekly,


--phone use performance
phone_use_self_performance_code as phone_use_self_performance,

case when phone_use_self_performance_code in ('By others','Full help','Some help - help some of the time') then 1 else 0
      end as phone_use_self_performance_code,

case when phone_use_self_performance_code = "Activity did not occur" then 0
when phone_use_self_performance_code = "Independent" then 0
when phone_use_self_performance_code = "Some help - help some of the time" then 0
when phone_use_self_performance_code = "Full help" then 0
when phone_use_self_performance_code = "By others" then 0
     end as phone_use_self_performance_weekly,


--shopping performance
shopping_self_performance_code as shopping_self_performance,

case when shopping_self_performance_code in ('By others','Full help','Some help - help some of the time') then 1 else 0
     end as shopping_self_performance_code,

case when shopping_self_performance_code = "Activity did not occur" then 0
when shopping_self_performance_code = "Independent" then 0
when shopping_self_performance_code = "Some help - help some of the time" then 90
when shopping_self_performance_code = "Full help" then 90
when shopping_self_performance_code = "By others" then 90
     end as shopping_self_performance_weekly,


--transportation performance
transportation_self_performance_code as transportation_self_performance,

case when transportation_self_performance_code in ('By others','Full help','Some help - help some of the time') then 1 else 0 end as transportation_self_performance_code,

case when transportation_self_performance_code = "Activity did not occur" then 0
when transportation_self_performance_code = "Independent" then 0
when transportation_self_performance_code = "Some help - help some of the time" then 30
when transportation_self_performance_code = "Full help" then 60
when transportation_self_performance_code = "By others" then 60
      end as transportation_self_performance_weekly,


--urinary function performance
control_of_urinary_bladder_function,

case when control_of_urinary_bladder_function in ('Incontinent','Frequently incontinent','Occasionally incontinent','Usually continent') then 1 else 0
      end as control_of_urinary_bladder_function_code,

case when control_of_urinary_bladder_function = "Continent" then 0
when control_of_urinary_bladder_function = "Continent with catheter" then 0
when control_of_urinary_bladder_function = "Usually continent" then 6
when control_of_urinary_bladder_function = "Occasionally incontinent" then 42
when control_of_urinary_bladder_function = "Frequently incontinent" then 84
when control_of_urinary_bladder_function = "Incontinent" then 126
when control_of_urinary_bladder_function = "Did not occur" then 0
      end as control_of_urinary_bladder_function_weekly,


--control of bowel performance
control_of_bowel_movement,

case when control_of_bowel_movement in ('Usually continent','Occasionally incontinent incontinent','Frequently incontinent','Incontinent') then 1 else 0
      end as control_of_bowel_movement_code,

case when control_of_bowel_movement = "Continent" then 0
when control_of_bowel_movement = "Continent with ostomy" then 0
when control_of_bowel_movement = "Usually continent" then 45
when control_of_bowel_movement = "Occasionally incontinent" then 105
when control_of_bowel_movement = "Frequently incontinent" then 210
when control_of_bowel_movement = "Incontinent" then 280
when control_of_bowel_movement = "Did not occur" then 0
      end as control_of_bowel_movement_weekly,


--helper questions
case when does_member_have_key_informal_helper_primary = 'Yes' then true when does_member_have_key_informal_helper_primary = 'No'  then false
      end as does_member_have_key_informal_helper_primary,

case when lives_with_client_key_informal_helper = 'Yes' then true when lives_with_client_key_informal_helper = 'No' then false
      end as lives_with_client_key_informal_helper,

case when adl_care_areas_of_help = 'Yes' then true  when adl_care_areas_of_help = 'No' then false
      end as adl_care_areas_of_help,

case when does_member_have_key_informal_helper_secondary = 'Yes' then true when does_member_have_key_informal_helper_secondary = 'No' then false
      end as does_member_have_key_informal_helper_secondary,

case when lives_with_client_secondary_key_informal_helper = 'Yes' then true when lives_with_client_secondary_key_informal_helper = 'No' then false
      end as lives_with_client_secondary_key_informal_helper,

case when adl_care_secondary_areas_of_help = 'Yes' then true when adl_care_secondary_areas_of_help = 'No' then false
      end as adl_care_secondary_areas_of_help,

-- Mental retardation with severe learning disability
case when does_the_member_have_any_mental_illness_mds ='Mental retardation' and any_psychiatric_diagnosis = 'Severe learning disability' then true
      end as Mental_retardation_with_severe_learning_disability1,

-- Severely impaired decision-making around organizing day
case when how_well_client_made_decision_about_organizing_day =	"Severely Impaired" then true
      end as Severely_impaired_decision_making_around_organizing_day,

-- Alzheiemers diagnosis
case when alzheiemers in (
"Alzheimer's	Present - not subject to focused treatment or monitoring by home care professional"
,"Alzheimer's	Present - monitored or treated by home care professional" ) then true
     end as Alzheiemers_diagnosis,

-- Other dementia diagnosis
case when
dementia_other_than_alzheiemers_disease in (
"Dementia other than Alzheimer's disease	Present - not subject to focused treatment or monitoring by home care professional"
,"Dementia other than Alzheimer's disease	Present - monitored or treated by home care professional" ) then true
      end as Other_dementia_diagnosis,

-- Total dependence for toileting, transfers, or locomotion in home
case when trim(toilet_use_self_performance) ='Total dependence' OR
          trim(transfer_self_performance) = 'Total dependence' OR
          trim(locomotion_in_home_self_performance_code) = 'Total dependence' then true
          end as Total_dependence_toileting_transfers_locomotion_in_home,

---CB Code
case when
diagnoses_codes_descriptions like '%G80.0 %' OR diagnoses_codes_descriptions like '%G82.50 %'
OR diagnoses_codes_descriptions like '%G82.51 %' OR diagnoses_codes_descriptions like '%G82.52 %'
OR diagnoses_codes_descriptions like '%G82.53 %' OR diagnoses_codes_descriptions like '%G82.54 %'
OR diagnoses_codes_descriptions like '%G82.50 %' OR diagnoses_codes_descriptions like '%G71.0 %'
OR diagnoses_codes_descriptions like '%G71.2 %' OR diagnoses_codes_descriptions like '%G12.21 %'
OR diagnoses_codes_descriptions like '%Z99.11 %'
then diagnoses_codes_descriptions
		else 'Dx Field Empty'
      end as C3B_Diagnosis_Codes,

case when latestRatingCategory = 'DC3B' then true
      end as Member_Latest_RC_is_C3B,

case when who_lived_with_at_time_of_referral ='Lived alone' then true
      end as Member_lives_alone,

case when length_of_time_alone_during_day ='All the time' then true
      end as Member_is_alone_all_the_time_during_day,

case when length_of_time_alone_during_day ='Long periods of time - e.g., all morning' then true
      end as Member_is_alone_for_long_periods_of_time_during_day,

case when does_the_member_have_any_mental_illness_mds ='Mental retardation' and any_psychiatric_diagnosis = 'Severe learning disability' then true
      end as Mental_retardation_with_severe_learning_disability,

case when diagnoses_codes_descriptions like '%F10.20 %'
OR diagnoses_codes_descriptions like '%F10.220 %' OR diagnoses_codes_descriptions like '%F10.221 %' OR diagnoses_codes_descriptions like '%F10.229 %'
OR diagnoses_codes_descriptions like '%F10.230 %' OR diagnoses_codes_descriptions like '%F10.231 %' OR diagnoses_codes_descriptions like '%F10.232 %'
OR diagnoses_codes_descriptions like '%F10.239 %' OR diagnoses_codes_descriptions like '%F10.24 %' OR diagnoses_codes_descriptions like '%F10.250 %'
OR diagnoses_codes_descriptions like '%F10.251 %' OR diagnoses_codes_descriptions like '%F10.259 %' OR diagnoses_codes_descriptions like '%F10.26 %'
OR diagnoses_codes_descriptions like '%F10.27 %' OR diagnoses_codes_descriptions like '%F10.280 %' OR diagnoses_codes_descriptions like '%F10.281 %'
OR diagnoses_codes_descriptions like '%F10.282 %' OR diagnoses_codes_descriptions like '%F10.288 %' OR diagnoses_codes_descriptions like '%F10.29 %'
OR diagnoses_codes_descriptions like '%F11.20 %' OR diagnoses_codes_descriptions like '%F11.220 %' OR diagnoses_codes_descriptions like '%F11.221 %'
OR diagnoses_codes_descriptions like '%F11.222 %' OR diagnoses_codes_descriptions like '%F11.23 %' OR diagnoses_codes_descriptions like '%F11.24 %'
OR diagnoses_codes_descriptions like '%F11.250 %' OR diagnoses_codes_descriptions like '%F11.251 %' OR diagnoses_codes_descriptions like '%F11.259 %'
OR diagnoses_codes_descriptions like '%F11.281 %' OR diagnoses_codes_descriptions like '%F11.282 %' OR diagnoses_codes_descriptions like '%F11.288 %'
OR diagnoses_codes_descriptions like '%F11.29 %' OR diagnoses_codes_descriptions like '%F12.20 %' OR diagnoses_codes_descriptions like '%F12.220 %'
OR diagnoses_codes_descriptions like '%F12.221 %' OR diagnoses_codes_descriptions like '%F12.222 %' OR diagnoses_codes_descriptions like '%F12.229 %'
OR diagnoses_codes_descriptions like '%F12.250 %' OR diagnoses_codes_descriptions like '%F12.251 %' OR diagnoses_codes_descriptions like '%F12.259 %'
OR diagnoses_codes_descriptions like '%F12.280 %' OR diagnoses_codes_descriptions like '%F12.288 %' OR diagnoses_codes_descriptions like '%F12.29 %'
OR diagnoses_codes_descriptions like '%F13.20 %' OR diagnoses_codes_descriptions like '%F13.220 %' OR diagnoses_codes_descriptions like '%F13.221 %'
OR diagnoses_codes_descriptions like '%F13.229 %' OR diagnoses_codes_descriptions like '%F13.230 %' OR diagnoses_codes_descriptions like '%F13.231 %'
OR diagnoses_codes_descriptions like '%F13.232 %' OR diagnoses_codes_descriptions like '%F13.239 %' OR diagnoses_codes_descriptions like '%F13.24 %'
OR diagnoses_codes_descriptions like '%F13.250 %' OR diagnoses_codes_descriptions like '%F13.251 %' OR diagnoses_codes_descriptions like '%F13.259 %'
OR diagnoses_codes_descriptions like '%F13.26 %' OR diagnoses_codes_descriptions like '%F13.27 %' OR diagnoses_codes_descriptions like '%F13.280 %'
OR diagnoses_codes_descriptions like '%F13.281 %' OR diagnoses_codes_descriptions like '%F13.282 %' OR diagnoses_codes_descriptions like '%F13.288 %'
OR diagnoses_codes_descriptions like '%F13.29 %' OR diagnoses_codes_descriptions like '%F14.20 %' OR diagnoses_codes_descriptions like '%F14.220 %'
OR diagnoses_codes_descriptions like '%F14.221 %' OR diagnoses_codes_descriptions like '%F14.222 %' OR diagnoses_codes_descriptions like '%F14.229 %'
OR diagnoses_codes_descriptions like '%F14.23 %' OR diagnoses_codes_descriptions like '%F14.24 %' OR diagnoses_codes_descriptions like '%F14.250 %'
OR diagnoses_codes_descriptions like '%F14.251 %' OR diagnoses_codes_descriptions like '%F14.259 %' OR diagnoses_codes_descriptions like '%F14.280 %'
OR diagnoses_codes_descriptions like '%F14.281 %' OR diagnoses_codes_descriptions like '%F14.282 %' OR diagnoses_codes_descriptions like '%F14.288 %'
OR diagnoses_codes_descriptions like '%F14.29 %' OR diagnoses_codes_descriptions like '%F15.20 %' OR diagnoses_codes_descriptions like '%F15.220 %'
OR diagnoses_codes_descriptions like '%F15.221 %' OR diagnoses_codes_descriptions like '%F15.222 %' OR diagnoses_codes_descriptions like '%F15.229 %'
OR diagnoses_codes_descriptions like '%F15.23 %' OR diagnoses_codes_descriptions like '%F15.24 %' OR diagnoses_codes_descriptions like '%F15.250 %'
OR diagnoses_codes_descriptions like '%F15.251 %' OR diagnoses_codes_descriptions like '%F15.259 %' OR diagnoses_codes_descriptions like '%F15.280 %'
OR diagnoses_codes_descriptions like '%F15.281 %' OR diagnoses_codes_descriptions like '%F15.282 %' OR diagnoses_codes_descriptions like '%F15.288 %'
OR diagnoses_codes_descriptions like '%F15.29 %' OR diagnoses_codes_descriptions like '%F16.20 %' OR diagnoses_codes_descriptions like '%F16.220 %'
OR diagnoses_codes_descriptions like '%F16.221 %' OR diagnoses_codes_descriptions like '%F16.229 %' OR diagnoses_codes_descriptions like '%F16.24 %'
OR diagnoses_codes_descriptions like '%F16.250 %' OR diagnoses_codes_descriptions like '%F16.251 %' OR diagnoses_codes_descriptions like '%F16.259 %'
OR diagnoses_codes_descriptions like '%F16.280 %' OR diagnoses_codes_descriptions like '%F16.283 %' OR diagnoses_codes_descriptions like '%F16.288 %'
OR diagnoses_codes_descriptions like '%F16.29 %' OR diagnoses_codes_descriptions like '%F18.20 %' OR diagnoses_codes_descriptions like '%F18.220 %'
OR diagnoses_codes_descriptions like '%F18.221 %' OR diagnoses_codes_descriptions like '%F18.229 %' OR diagnoses_codes_descriptions like '%F18.24 %'
OR diagnoses_codes_descriptions like '%F18.250 %' OR diagnoses_codes_descriptions like '%F18.251 %' OR diagnoses_codes_descriptions like '%F18.259 %'
OR diagnoses_codes_descriptions like '%F18.27 %' OR diagnoses_codes_descriptions like '%F18.280 %' OR diagnoses_codes_descriptions like '%F18.288 %'
OR diagnoses_codes_descriptions like '%F18.29 %' OR diagnoses_codes_descriptions like '%F19.20 %' OR diagnoses_codes_descriptions like '%F19.220 %'
OR diagnoses_codes_descriptions like '%F19.221 %' OR diagnoses_codes_descriptions like '%F19.222 %' OR diagnoses_codes_descriptions like '%F19.229 %'
OR diagnoses_codes_descriptions like '%F19.230 %' OR diagnoses_codes_descriptions like '%F19.231 %' OR diagnoses_codes_descriptions like '%F19.232 %'
OR diagnoses_codes_descriptions like '%F19.239 %' OR diagnoses_codes_descriptions like '%F19.24 %' OR diagnoses_codes_descriptions like '%F19.250 %'
OR diagnoses_codes_descriptions like '%F19.251 %' OR diagnoses_codes_descriptions like '%F19.259 %' OR diagnoses_codes_descriptions like '%F19.26 %'
OR diagnoses_codes_descriptions like '%F19.27 %' OR diagnoses_codes_descriptions like '%F19.280 %' OR diagnoses_codes_descriptions like '%F19.281 %'
OR diagnoses_codes_descriptions like '%F19.282 %' OR diagnoses_codes_descriptions like '%F19.288 %' OR diagnoses_codes_descriptions like '%F19.29 %'
then true
    end as Substance_Use_Code_Given,

case when diagnoses_codes_descriptions like '%F20.89 %'
OR diagnoses_codes_descriptions like '%F20.1 %' OR diagnoses_codes_descriptions like '%F20.2 %' OR diagnoses_codes_descriptions like '%F20.0 %'
OR diagnoses_codes_descriptions like '%F20.81 %' OR diagnoses_codes_descriptions like '%F20.5 %' OR diagnoses_codes_descriptions like '%F25.0 %'
OR diagnoses_codes_descriptions like '%F25.1 %' OR diagnoses_codes_descriptions like '%F25.8 %' OR diagnoses_codes_descriptions like '%F25.9 %'
OR diagnoses_codes_descriptions like '%F20.3 %' OR diagnoses_codes_descriptions like '%F20.9 %' OR diagnoses_codes_descriptions like '%F30.10 %'
OR diagnoses_codes_descriptions like '%F30.12 %' OR diagnoses_codes_descriptions like '%F30.2 %' OR diagnoses_codes_descriptions like '%F30.3 %'
OR diagnoses_codes_descriptions like '%F30.4 %' OR diagnoses_codes_descriptions like '%F30.8 %' OR diagnoses_codes_descriptions like '%F30.9 %'
OR diagnoses_codes_descriptions like '%F31.0 %' OR diagnoses_codes_descriptions like '%F31.10 %' OR diagnoses_codes_descriptions like '%F31.11 %'
OR diagnoses_codes_descriptions like '%F31.12 %' OR diagnoses_codes_descriptions like '%F31.13 %' OR diagnoses_codes_descriptions like '%F31.2 %'
OR diagnoses_codes_descriptions like '%F31.30 %' OR diagnoses_codes_descriptions like '%F31.31 %' OR diagnoses_codes_descriptions like '%F31.32 %'
OR diagnoses_codes_descriptions like '%F31.4 %' OR diagnoses_codes_descriptions like '%F31.5 %' OR diagnoses_codes_descriptions like '%F31.60 %'
OR diagnoses_codes_descriptions like '%F31.61 %' OR diagnoses_codes_descriptions like '%F31.62 %' OR diagnoses_codes_descriptions like '%F31.63 %'
OR diagnoses_codes_descriptions like '%F31.64 %' OR diagnoses_codes_descriptions like '%F31.70 %' OR diagnoses_codes_descriptions like '%F31.71 %'
OR diagnoses_codes_descriptions like '%F31.72 %' OR diagnoses_codes_descriptions like '%F31.73 %' OR diagnoses_codes_descriptions like '%F31.74 %'
OR diagnoses_codes_descriptions like '%F31.75 %' OR diagnoses_codes_descriptions like '%F31.76 %' OR diagnoses_codes_descriptions like '%F31.77 %'
OR diagnoses_codes_descriptions like '%F31.78 %' OR diagnoses_codes_descriptions like '%F31.81 %' OR diagnoses_codes_descriptions like '%F31.89 %'
OR diagnoses_codes_descriptions like '%F31.9 %' OR diagnoses_codes_descriptions like '%F32.0 %' OR diagnoses_codes_descriptions like '%F32.1 %'
OR diagnoses_codes_descriptions like '%F32.2 %' OR diagnoses_codes_descriptions like '%F32.3 %' OR diagnoses_codes_descriptions like '%F32.4 %'
OR diagnoses_codes_descriptions like '%F32.5 %' OR diagnoses_codes_descriptions like '%F32.8 %' OR diagnoses_codes_descriptions like '%F32.9 %'
OR diagnoses_codes_descriptions like '%F33.0 %' OR diagnoses_codes_descriptions like '%F33.1 %' OR diagnoses_codes_descriptions like '%F33.2 %'
OR diagnoses_codes_descriptions like '%F33.3 %' OR diagnoses_codes_descriptions like '%F33.40 %' OR diagnoses_codes_descriptions like '%F33.41 %'
OR diagnoses_codes_descriptions like '%F33.42 %' OR diagnoses_codes_descriptions like '%F33.8 %' OR diagnoses_codes_descriptions like '%F33.9 %'
OR diagnoses_codes_descriptions like '%F34.8 %' OR diagnoses_codes_descriptions like '%F34.9 %' OR diagnoses_codes_descriptions like '%F39 %'
OR diagnoses_codes_descriptions like '%F28 %' OR diagnoses_codes_descriptions like '%F29 %'
then true
        end as Behaviorial_Code_Given,

case when diagnoses_codes_descriptions like '%G80.0 %' OR diagnoses_codes_descriptions like '%G82.50 %'
          OR diagnoses_codes_descriptions like '%G82.51 %' OR diagnoses_codes_descriptions like '%G82.52 %'
          OR diagnoses_codes_descriptions like '%G82.53 %' OR diagnoses_codes_descriptions like '%G82.54 %'
          OR diagnoses_codes_descriptions like '%G82.50 %' OR diagnoses_codes_descriptions like '%G71.0 %'
          OR diagnoses_codes_descriptions like '%G71.2 %' OR diagnoses_codes_descriptions like '%G12.21 %'
          OR diagnoses_codes_descriptions like '%Z99.11 %' OR diagnoses_codes_descriptions like '%Z99.12 %'
          then true
      end as C3B_Code_Given,

---"Non-Qualifying BH indicators: These indicate that a BH code may exist for a member not currently captured in MDS"
case when does_the_member_have_any_mental_illness_mds = 'Mental Illness' then true
      end as Mental_Illness,

case when does_the_member_have_any_mental_illness_mds ='Mental retardation'	then true
      end as  Mental_Retardation,

case when any_psychiatric_diagnosis in ( 'Severe learning disability', 'Spina bifida', 'Spinal cord injury  Major mental illness prior to 22') then true
      end as Major_mental_illness_prior_to_22,

case when any_psychiatric_diagnosis in ('Present - not subject to focused treatment or monitoring by home care professional',
'Present - monitored or treated by home care professional') then true
      end as Any_psychiatric_diagnosis,

case when alcohol_or_drug_treatment_program in ('Scheduled, full adherence as prescribed', 'Scheduled, partial adherence', 'Scheduled, not received') then true
      end as Alcohol_drug_treatment_program,

case when antidepressant_medication = 'Yes' then true
      end as Antidepressant_medication,

case when antipsychotic_medication = 'Yes' then true
      end as Antipsychotic_medication

from
transposed),


services_applicable as (

select
*,

(mobility_in_bed_self_performance_code + transfer_self_performance_code + locomotion_in_home_self_performance_code + locomotion_outside_of_home_self_performance_code + dressing_lower_body_self_performance_code + dressing_upper_body_self_performance_code + eating_self_performance_code + personal_hygiene_self_performance_code + bathing_self_performance_code + toilet_use_self_performance_code + control_of_urinary_bladder_function_code + control_of_bowel_movement_code)
as ADL_categories,

(mobility_in_bed_minutes_week + transfer_self_performance_minutes_week + locomotion_in_home_self_performance_week + locomotion_outside_of_home_self_performance_week + dressing_upper_body_self_performance_week + dressing_lower_body_self_performance_week + eating_self_performance_week + personal_hygiene_self_performance_week + bathing_self_performance_week + toilet_use_self_performance_week + control_of_urinary_bladder_function_weekly + control_of_bowel_movement_weekly)
as ADL_Minutes_per_week,

(mobility_in_bed_minutes_week + transfer_self_performance_minutes_week + locomotion_in_home_self_performance_week + locomotion_outside_of_home_self_performance_week + dressing_upper_body_self_performance_week + dressing_lower_body_self_performance_week + eating_self_performance_week + personal_hygiene_self_performance_week + bathing_self_performance_week + toilet_use_self_performance_week + control_of_urinary_bladder_function_weekly + control_of_bowel_movement_weekly)/60
as ADL_Hours_per_week,

(meal_preparation_self_performance_code + ordinary_housework_self_performance_code + managing_finance_self_performance_code + managing_medications_self_performance_code + phone_use_self_performance_code + shopping_self_performance_code + transportation_self_performance_code)
as IADL_categories,

(meal_preparation_self_performance_weekly + ordinary_housework_self_performance_weekly + managing_finance_self_performance_weekly + managing_medications_self_performance_weekly + phone_use_self_performance_weekly + shopping_self_performance_weekly + transportation_self_performance_weekly)
as IADL_Minutes_per_week,

(meal_preparation_self_performance_weekly + ordinary_housework_self_performance_weekly + managing_finance_self_performance_weekly + managing_medications_self_performance_weekly + phone_use_self_performance_weekly + shopping_self_performance_weekly + transportation_self_performance_weekly)/60
as IADL_Hours_per_week,

case when locomotion_in_home_self_performance in ('Limited assistance','Extensive assistance','Maximal assistance', 'Total dependence')
or toilet_use_self_performance in ('Limited assistance','Extensive assistance','Maximal assistance','Total dependence')
then true
end as Member_needs_hands_on_home_assistance,

Member_lives_alone as Member_lives_alone1,
case when (lives_with_client_key_informal_helper = true and  adl_care_areas_of_help = true )
or (lives_with_client_secondary_key_informal_helper = true and  adl_care_secondary_areas_of_help = true )
then true
end as Member_has_informal_helper_who_can_assist,


case when Behaviorial_Code_Given = true or Substance_Use_Code_Given = true then true
      end as Documented_MDS_qualifying_BH_indication_for_ADLs,

case when Mental_retardation_with_severe_learning_disability1 = true
OR Severely_impaired_decision_making_around_organizing_day = true
OR Alzheiemers_diagnosis = true
OR Other_dementia_diagnosis = true
OR Total_dependence_toileting_transfers_locomotion_in_home = true
OR C3B_Diagnosis_Codes <> 'Dx Field Empty'
OR Member_Latest_RC_is_C3B = true then true
    end as Does_the_member_have_conditions_that_may_require_24_hour_supervision,


case when (Mental_retardation_with_severe_learning_disability1 = true
OR Severely_impaired_decision_making_around_organizing_day = true
OR Alzheiemers_diagnosis = true
OR Other_dementia_diagnosis = true
OR Total_dependence_toileting_transfers_locomotion_in_home = true
OR C3B_Diagnosis_Codes <> 'Dx Field Empty'
OR Member_Latest_RC_is_C3B = true)
and
Member_lives_alone = true then true
     end as Does_member_have_overnight_supervisions_gaps,

case when (Mental_retardation_with_severe_learning_disability1 = true
OR Severely_impaired_decision_making_around_organizing_day = true
OR Alzheiemers_diagnosis = true
OR Other_dementia_diagnosis = true
OR Total_dependence_toileting_transfers_locomotion_in_home = true
OR C3B_Diagnosis_Codes <> 'Dx Field Empty'
OR Member_Latest_RC_is_C3B = true)
and
(Member_is_alone_for_long_periods_of_time_during_day or Member_is_alone_all_the_time_during_day = true) then true
      end as Does_the_member_have_long_daytime_supervision_gaps

from
compiled
),


product as (
select

case when ADL_categories >=1 OR
 (lower(mobility_in_bed_self_performance) like '%supervision%' or
 lower(transfer_self_performance) like '%supervision%' or
 lower(locomotion_in_home_self_performance) like '%supervision%' or
 locomotion_outside_of_home_self_performance like '%supervision%' or
 dressing_upper_body_self_performance like '%supervision%' or
 dressing_lower_body_self_performance like '%supervision%' or
 eating_self_performance like '%supervision%' or
 toilet_use_self_performance like '%supervision%' or
 personal_hygiene_self_performance like '%supervision%' or
 bathing_self_performance like '%supervision%')
 then true end as Adult_Foster_Care,

 case when ADL_categories >=1 OR
 (managing_medications_self_performance_weekly >0 ) OR
 (lower(mobility_in_bed_self_performance) like '%supervision%' or
 lower(transfer_self_performance) like '%supervision%' or
 lower(locomotion_in_home_self_performance) like '%supervision%' or
 locomotion_outside_of_home_self_performance like '%supervision%' or
 dressing_upper_body_self_performance like '%supervision%' or
 dressing_lower_body_self_performance like '%supervision%' or
 eating_self_performance like '%supervision%' or
 toilet_use_self_performance like '%supervision%' or
 personal_hygiene_self_performance like '%supervision%' or
 bathing_self_performance like '%supervision%')
 then true  end as Group_Adult_Foster_Care,


 --same as above
 case when ADL_categories >=1 OR
 (lower(mobility_in_bed_self_performance) like '%supervision%' or
 lower(transfer_self_performance) like '%supervision%' or
 lower(locomotion_in_home_self_performance) like '%supervision%' or
 locomotion_outside_of_home_self_performance like '%supervision%' or
 dressing_upper_body_self_performance like '%supervision%' or
 dressing_lower_body_self_performance like '%supervision%' or
 eating_self_performance like '%supervision%' or
 toilet_use_self_performance like '%supervision%' or
 personal_hygiene_self_performance like '%supervision%' or
 bathing_self_performance like '%supervision%')
 then true end as Adult_Day_Health,

case when IADL_categories >=1 then true
      end as Homemaker,

case when ADL_categories >=2 then true
      end as PCA,

case when ADL_categories >=1 then true
      end as PC,

case when ADL_categories >=1 then true
      end as Home_Health_Aide,

case when (mobility_in_bed_minutes_week + transfer_self_performance_minutes_week + locomotion_in_home_self_performance_week + locomotion_outside_of_home_self_performance_week + dressing_upper_body_self_performance_week + dressing_lower_body_self_performance_week + eating_self_performance_week + personal_hygiene_self_performance_week + bathing_self_performance_week + toilet_use_self_performance_week + +control_of_bowel_movement_weekly + control_of_urinary_bladder_function_weekly + meal_preparation_self_performance_weekly + ordinary_housework_self_performance_weekly + managing_finance_self_performance_weekly + managing_medications_self_performance_weekly + phone_use_self_performance_weekly + shopping_self_performance_weekly + transportation_self_performance_weekly) >1
or
( Member_Latest_RC_is_C3B is true or Other_dementia_diagnosis = true or C3B_Diagnosis_Codes <> 'Dx Field Empty'  or Severely_impaired_decision_making_around_organizing_day	= true or Alzheiemers_diagnosis=true or Other_dementia_diagnosis=true or Mental_retardation_with_severe_learning_disability1=true )
THEN TRUE end as Companion,

case when shopping_self_performance_weekly + meal_preparation_self_performance_weekly >0 then true
    end as Meal_Delivery,

case when ordinary_housework_self_performance_weekly >0 then true
    end as Chore,

case when ordinary_housework_self_performance_weekly >0 then true
    end as Laundry,

case when shopping_self_performance_weekly >0 then true
      end as Groceries,

case when managing_medications_self_performance_weekly >0 then true
      end as Med_Box,

case when
(mobility_in_bed_minutes_week + transfer_self_performance_minutes_week + locomotion_in_home_self_performance_week + locomotion_outside_of_home_self_performance_week + dressing_upper_body_self_performance_week + dressing_lower_body_self_performance_week + eating_self_performance_week + personal_hygiene_self_performance_week + bathing_self_performance_week + toilet_use_self_performance_week + meal_preparation_self_performance_weekly + ordinary_housework_self_performance_weekly + managing_finance_self_performance_weekly + managing_medications_self_performance_weekly + phone_use_self_performance_weekly + shopping_self_performance_weekly + transportation_self_performance_weekly) >=1 then true
      end as Life_Skills_Training,

case when Member_needs_hands_on_home_assistance = true  and Member_lives_alone =true then true else false
      end as Overnight_PCA_recommended,

case when Member_needs_hands_on_home_assistance = true  and Member_lives_alone = false then true else false
      end as Overnight_PCA_recommended_some_nights,
 *
from
services_applicable),



final as (

select distinct
concat(patientId, patientName) as Patient_name_and_ID,
patientId,
patientName,
cohortName,
latestRatingCategory,
member_ready_for_review,
assessment_reference_date_time,
RN_Completing_MDS,

case when ADL_categories >= 4 then "Yes, the member has 4 or more ADLs. If the member needs additional hands-on support, PCA is appropriate"
when ADL_categories between 2 and 3	then "The member has 2-3 ADLs. If the member is interested in PCA or you believe it is the best approach, refer for a PCA eval. Other approaches such as PC or HM could be considered if the member has home supports."
when ADL_categories = 1	then "The member has 1 documented ADL and does not qualify for PCA (2+ ADLs needed)"
when ADL_categories = 0 then "The member has 0 documented ADLs and does not qualify for PCA (2+ ADLs needed)"
when ADL_categories between 0 and 1 then "If this member receives PCA or you believe member needs PCA based on hands-on needs, please notify Kristen McNeil so that she can determine if a reassessment is necessary"
when ADL_categories between 0 and 1  and Documented_MDS_qualifying_BH_indication_for_ADLs is false then "BH condition indicated but no code, some supervisory needs	This member looks like they may be missing a BH code, which could increase the level of services they qualify for"
end as Should_PCA_evaluation_be_offered,

case when ADL_categories =4 then "PCA"
when ADL_categories >= 2 and ADL_Hours_per_week>=14 then "PCA"
when ADL_categories>=1 and ADL_Hours_per_week between 8 and 14 then "PC or Homemaking"
when ADL_Hours_per_week>0 then "Consider ad hoc services first. Homemaking can be added if needed." else "No noted LTSS needs"
      end as which_services_first,

case when does_member_have_key_informal_helper_primary = true or does_member_have_key_informal_helper_secondary	= true  then "Member has informal helper who can help with ADLs" else "No"
      end as Does_Member_have_informal_helper,

"Do not share this number with member or externally until reviewing PCA evaluation and recommended hours" as  how_many_hours_is_appropriate,

round((ADL_Hours_per_week) + (IADL_Hours_per_week),2) as Weekly_daytime_hours,

round(case when Overnight_PCA_recommended = true then 14
when Overnight_PCA_recommended_some_nights = true then 6
else 0 end,2) as  Overnight_Hours_Recommended,

round((ADL_Hours_per_week) + (IADL_Hours_per_week )+

(case when Overnight_PCA_recommended = true then 14
when Overnight_PCA_recommended_some_nights =true then 6
else 0 end),2)
as total_weekly_hours,

Does_the_member_have_conditions_that_may_require_24_hour_supervision as Does_the_member_have_conditions_that_may_require_24_hour_supervision1,

Adult_Foster_care,
Group_Adult_Foster_Care,

Adult_Day_Health,
Homemaker,
PCA,
PC,
Home_Health_Aide,
Companion,
Meal_Delivery,
Chore,
Laundry,
Groceries,
Med_Box,
Life_Skills_Training,

ADL_categories,
IADL_categories,

round(ADL_Hours_per_week,2)  as ADL_Hours_Recommended,
round(IADL_Hours_per_week,2) as IADL_Hours_Recommended,

mobility_in_bed_self_performance,
mobility_in_bed_self_performance_code,
mobility_in_bed_minutes_week,
transfer_self_performance,
transfer_self_performance_code,
transfer_self_performance_minutes_week,
locomotion_in_home_self_performance,
locomotion_in_home_self_performance_code,
locomotion_in_home_self_performance_week,
locomotion_outside_of_home_self_performance,
locomotion_outside_of_home_self_performance_code,
locomotion_outside_of_home_self_performance_week,
dressing_upper_body_self_performance,
dressing_upper_body_self_performance_code,
dressing_upper_body_self_performance_week,
dressing_lower_body_self_performance,
dressing_lower_body_self_performance_code,
dressing_lower_body_self_performance_week,
eating_self_performance,
eating_self_performance_code,
eating_self_performance_week,
toilet_use_self_performance,
toilet_use_self_performance_code,
toilet_use_self_performance_week,
personal_hygiene_self_performance,
personal_hygiene_self_performance_code,
personal_hygiene_self_performance_week,
bathing_self_performance,
bathing_self_performance_code,
bathing_self_performance_week,
meal_preparation_self_performance,
meal_preparation_self_performance_code,
meal_preparation_self_performance_weekly,
ordinary_housework_self_performance,
ordinary_housework_self_performance_code,
ordinary_housework_self_performance_weekly,
managing_finance_self_performance,
managing_finance_self_performance_code,
managing_finance_self_performance_weekly,
managing_medications_self_performance,
managing_medications_self_performance_code,
managing_medications_self_performance_weekly,
phone_use_self_performance,
phone_use_self_performance_code,
phone_use_self_performance_weekly,
shopping_self_performance,
shopping_self_performance_code,
shopping_self_performance_weekly,
transportation_self_performance,
transportation_self_performance_code,
transportation_self_performance_weekly,
control_of_urinary_bladder_function,
control_of_urinary_bladder_function_code,
control_of_urinary_bladder_function_weekly,
control_of_bowel_movement,
control_of_bowel_movement_code,
control_of_bowel_movement_weekly,
does_member_have_key_informal_helper_primary,
lives_with_client_key_informal_helper,
adl_care_areas_of_help,
does_member_have_key_informal_helper_secondary,
lives_with_client_secondary_key_informal_helper,
adl_care_secondary_areas_of_help,
Mental_retardation_with_severe_learning_disability1,
Severely_impaired_decision_making_around_organizing_day,
Alzheiemers_diagnosis,
Other_dementia_diagnosis,
C3B_Diagnosis_Codes,
Member_Latest_RC_is_C3B,
Member_lives_alone,
Member_is_alone_all_the_time_during_day,
Member_is_alone_for_long_periods_of_time_during_day,
Mental_retardation_with_severe_learning_disability,
Substance_Use_Code_Given,
Behaviorial_Code_Given,
C3B_Code_Given,
Mental_Illness,
Mental_Retardation,
Major_mental_illness_prior_to_22,
Any_psychiatric_diagnosis,
Alcohol_drug_treatment_program,
Antidepressant_medication,
Antipsychotic_medication,

ADL_Minutes_per_week,
round(ADL_Hours_per_week,2) as ADL_Hours_per_week,

IADL_Minutes_per_week,
round(IADL_Hours_per_week,2) as IADL_Hours_per_week,

Overnight_PCA_recommended,
Overnight_PCA_recommended_some_nights,
Member_needs_hands_on_home_assistance,
Member_lives_alone1,
Member_has_informal_helper_who_can_assist,
Documented_MDS_qualifying_BH_indication_for_ADLs,
Does_the_member_have_conditions_that_may_require_24_hour_supervision,
Does_member_have_overnight_supervisions_gaps,
Does_the_member_have_long_daytime_supervision_gaps

from
product
)

select * from final