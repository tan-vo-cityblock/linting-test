-- Assessments-related info - resulting table has patientId and completion timestamps for fall, physical activity, urinary incontinence, and depression screens

with mainassessments as (
    SELECT
    pa.patientId,
    pa.questionSlug,
    pa.answerSlug,
    psts.screeningToolSlug,
    raas.assessmentSlug,
    COALESCE(ast.riskAreaGroupSlug, st.riskAreaGroupSlug) as riskAreaGroupSlug,
    pa.id as patientAnswerId,
    pa.patientScreeningToolSubmissionId,
    pa.riskAreaAssessmentSubmissionId,
    case when raas.completedAt between  rd.reporting_datetime_first and rd.reporting_datetime_last then raas.completedAt
      else null end as riskAreaAssessmentCompletedAt,
    case when psts.scoredAt between  rd.reporting_datetime_first and rd.reporting_datetime_last  then psts.scoredAt
      else null end as screeningToolCompletedAt,
    case when pa.deletedAt between  rd.reporting_datetime_first and rd.reporting_datetime_last  then pa.deletedAt
     else null end as patientAnswerDeletedAt,
    case when psts.deletedAt between  rd.reporting_datetime_first and rd.reporting_datetime_last  then psts.deletedAt
      else null end as screeningToolDeletedAt,
    case when raas.deletedAt between  rd.reporting_datetime_first and rd.reporting_datetime_last  then raas.deletedAt
      else null end as riskAreaAssessmentDeletedAt
FROM {{ source('commons', 'patient_answer') }} pa
LEFT JOIN {{ source('commons', 'patient_screening_tool_submission') }} psts
ON pa.patientScreeningToolSubmissionId = psts.id AND psts.scoredAt is not null
LEFT JOIN {{ source('commons', 'screening_tool') }} st
ON psts.screeningToolSlug = st.slug
LEFT JOIN {{ source('commons', 'risk_area_assessment_submission') }} raas
ON pa.riskAreaAssessmentSubmissionId = raas.id AND raas.completedAt is not null
LEFT JOIN {{ source('commons', 'assessment') }} ast
ON raas.assessmentSlug = ast.slug
--we want to make sure createdAt date(s) is between delegation date AND reporting date.
--Other dates act the same but if they are after the reporting date, they are null
INNER JOIN {{ ref('cm_del_reporting_dates_monthly') }} rd
    on pa.createdAt between  rd.reporting_datetime_first and rd.reporting_datetime_last
INNER join {{ ref('cm_del_delegation_dates_ytd') }} dd
     on date(pa.createdat) >= delegation_at
     and pa.patientid = dd.patientid
WHERE (
     (pa.questionSlug in (
      --FALLs
        'have-you-fallen-in-the-past-year',
        'have-you-had-a-problem-with-balance-or-walking-in-the-past-year',
     --Physical Activity
        'how-often-moderated-physical-activity-nonstop-for-10-minutes',
     --URINARY INCONTINENCE
        'do-you-need-help-with-any-of-these-activities')
        and raas.completedAt is not null
     )
     OR
        --Completed screening tool
        --PHQ-9
        (psts.screeningToolSlug = 'depression-screening-phq-9'
        and psts.scoredAt is not null)
    )
),


-- Take earliest completion date for the questions and screening tool

earliestquestiondate as(
    select
        patientId,
        questionSlug,
        screeningToolSlug,
        assessmentSlug,
        COALESCE(
            MIN(case when screeningToolSlug is null then riskAreaAssessmentCompletedAt end),
            MIN(case when screeningToolSlug is not null then screeningToolCompletedAt end)
            ) as earliest_question_submission_date
    FROM mainassessments
    Group by 1,2,3,4
),

countquestions as (
    select
        patientId,
        assessmentSlug,
        screeningToolSlug,
        count (distinct (questionSlug) ) as count_questions
    from mainassessments
    group by 1,2,3
),

assessmentcomplete as (
    select
        cq.patientId,
        cq.assessmentSlug,
        cq.screeningToolSlug,
        ((cq.assessmentSlug = 'fall-risk' and cq.count_questions = 2)
                OR (cq.assessmentSlug = 'exercise' and cq.count_questions = 1)
                OR (cq.assessmentSlug = 'activities-of-daily-living' and cq.count_questions = 1)
                OR (cq.screeningToolSlug = 'depression-screening-phq-9' and cq.count_questions in (2,9))
            ) as assessment_complete
    from countquestions cq
),

assessmenttitle as(
    select
        m.patientId,
        m.assessmentSlug,
        m.screeningToolSlug,
        case
            when ac.assessment_complete = true THEN max(eqd.earliest_question_submission_date)
            else null end as assessment_completion_date,
        case
            when m.assessmentSlug = 'fall-risk'  then 'fall_prevention'
            when m.assessmentSlug = 'exercise' then 'physical_activity'
            when m.assessmentSlug = 'activities-of-daily-living' then 'urinary_incontinence'
            when m.screeningToolSlug = 'depression-screening-phq-9' then 'depression_phq9'
            else null end as assessment_title
    from mainassessments m
    left join assessmentcomplete ac
    on m.patientId = ac.patientId
      and (m.assessmentSlug = ac.assessmentSlug
      or m.screeningToolSlug = ac.screeningToolSlug)
    left join earliestquestiondate eqd
      on m.patientId = eqd.patientid
      and (m.assessmentSlug = eqd.assessmentSlug
      or m.screeningToolSlug = eqd.screeningToolSlug)
    where ac.assessment_complete = true
    group by 1,2,3, ac.assessment_complete,5
),

depression_phq9 as (select
    patientid,
    true as depression_phq9_assessment_complete,
    assessment_completion_date as depression_phq9_assessment_completion_date
    from assessmenttitle
    where assessment_title='depression_phq9'
    and assessment_title is not null
),

fall_prevention as (select
    patientid,
    true as fall_prevention_assessment_complete,
    assessment_completion_date as fall_prevention_assessment_completion_date
    from assessmenttitle
    where assessment_title='fall_prevention'
    and assessment_title is not null
),


urinary_incontinence as (select
    patientid,
    true as urinary_incontinence_assessment_complete,
    assessment_completion_date as urinary_incontinence_assessment_completion_date
    from assessmenttitle
    where assessment_title='urinary_incontinence'
    and assessment_title is not null
),

physical_activity as (select
    patientid,
    true as physical_activity_assessment_complete,
    assessment_completion_date as physical_activity_assessment_completion_date
    from assessmenttitle
    where assessment_title='physical_activity'
    and assessment_title is not null
)

select p.patientId,
coalesce(depression_phq9_assessment_complete,FALSE) as depression_phq9_assessment_complete,
date(depression_phq9_assessment_completion_date) as depression_phq9_assessment_completion_date,
coalesce(fall_prevention_assessment_complete,FALSE) as fall_prevention_assessment_complete,
date(fall_prevention_assessment_completion_date) as fall_prevention_assessment_completion_date,
coalesce(urinary_incontinence_assessment_complete,FALSE) as urinary_incontinence_assessment_complete,
date(urinary_incontinence_assessment_completion_date) as urinary_incontinence_assessment_completion_date,
coalesce(physical_activity_assessment_complete,FALSE) as physical_activity_assessment_complete,
date(physical_activity_assessment_completion_date) as physical_activity_assessment_completion_date,
current_date as date_run
from {{ ref('cm_del_patients_monthly') }} p
left join depression_phq9 using(patientId)
left join fall_prevention using(patientId)
left join urinary_incontinence using(patientId)
left join physical_activity using(patientId)
