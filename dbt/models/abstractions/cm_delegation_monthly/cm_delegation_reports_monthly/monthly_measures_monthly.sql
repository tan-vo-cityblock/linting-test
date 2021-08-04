select
pf.Memberid as member_id,
pf.Firstname as first_name,
pf.Lastname as last_name,
pf.Age,
Pf.line_of_business,
pf.cohortName as cohort_name,
cm.care_management_model,
coalesce(Pf.flu_shot_received,false) as flu_shot_received,
Pf.flu_shot_received_date,
coalesce(Pf.pneumonia_shot_received,false) as pneumonia_shot_received,
Pf.pneumonia_shot_received_date,
Assessments.depression_phq9_assessment_complete as bh_screening_received,
Assessments.depression_phq9_assessment_completion_date as bh_screening_date,
Assessments.fall_prevention_assessment_complete as fall_prevention_screening_received,
Assessments.fall_prevention_assessment_completion_date as fall_prevention_screening_date,
Assessments.physical_activity_assessment_complete as physical_activity_screening_received,
Assessments.physical_activity_assessment_completion_date as physical_activity_screening_date,
Assessments.urinary_incontinence_assessment_complete as urinary_incontinence_screening_received,
Assessments.urinary_incontinence_assessment_completion_date as urinary_incontinence_screening_date,
partner
From{{ ref('cm_del_patients_final_flags_monthly') }} pf
Left join {{ ref('cm_del_assessments_monthly') }} assessments
On Pf.patientid = assessments.patientid
Left join {{ ref('cm_del_care_management_model_ytd') }} cm
on Pf.patientid = cm.patientid
where (flu_shot_received is true or pneumonia_shot_received is true or
depression_phq9_assessment_complete is true or fall_prevention_assessment_complete is true
or physical_activity_assessment_complete is true or urinary_incontinence_assessment_complete is true)
