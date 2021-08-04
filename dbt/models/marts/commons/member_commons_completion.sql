with hcp_molst as (

    select
        patientId,
        CAST(pi.hasHealthcareProxy as BOOLEAN) as hasHealthcareProxy,
        CAST(pi.hasMolst as BOOLEAN) as hasMolst,

        CASE
          WHEN (CAST(pi.hasHealthcareProxy as BOOLEAN) = TRUE OR CAST(pi.hasMolst as BOOLEAN) = TRUE) THEN TRUE
          WHEN (CAST(pi.hasHealthcareProxy as BOOLEAN) = FALSE AND CAST(pi.hasMolst as BOOLEAN) = FALSE) THEN FALSE
          ELSE FALSE
          END
        AS hasAdvanceDirectives

    from {{ source('commons', 'patient_info') }} as pi

),

min_acp_document_upload as (
    SELECT 
        row.patientId,
        row.createdAt as minAcpDocumentUploadAt
    
    FROM (
        SELECT ARRAY_AGG(t order by createdAt ASC LIMIT 1)[OFFSET(0)] row
            FROM {{ source('commons', 'patient_document') }} t
            WHERE deletedAt IS NULL
            AND documentType in ('hcp','molst')
            GROUP BY patientId
    ) 

),

min_demographics as (

    select
        patientId,
        min(createdAt) as minDemographicsAt

    from {{ source('commons', 'computed_patient_status') }}

    where hasCompletedRequiredDemographics is true

    group by patientId

),

has_assessment as (

    select distinct
        patientId

    from {{ source('commons', 'risk_area_assessment_submission') }}

    where deletedAt IS NULL

),

has_any_answers as (

    select distinct
        patientId

    from {{ ref('questions_answers_current') }}

),

min_tasks as (

    select
        patientId,
        MIN(taskCreatedAt) AS minTaskAt,
        MIN(taskCompletedAt) AS minCompletedTaskAt,
        MAX(taskCreatedAt) AS maxTaskAt,
        MAX(taskCompletedAt) AS maxCompletedTaskAt,
        MAX(goalUpdatedAt) AS maxGoalUpdatedAt

    from {{ ref('member_goals_tasks') }}
    where taskIsAssociatedWithGoal is true

    group by patientId

),

care_plan as (

    select
        patientId,
        minTaskAt,

        CASE
            WHEN minTaskAt IS NOT NULL
            THEN True
            ELSE False
            END
            AS hasGeneratedCarePlan,

        minCompletedTaskAt,
        maxCompletedTaskAt,

        CASE WHEN minCompletedTaskAt IS NOT NULL
            THEN True
            ELSE False
            END
            AS hasCompletedOneTask,

        maxGoalUpdatedAt

    from min_tasks

),

min_acuity as (

    select
        patientId,
        MIN(createdAt) AS acuityAssignedAt

    from {{ source('commons', 'patient_acuity') }}

    where isRecommended = false

    GROUP BY patientId

),

agg_case_conference as (

    select
        patientId,
        min(eventTimestamp) AS minCaseConferenceAt,
        max(eventTimestamp) AS maxCaseConferenceAt,
        count(distinct memberInteractionKey) AS totalNumCaseConferenceEver

    from {{ ref('member_interactions') }}

    where (legacytitle = 'Case Conference' OR eventType = 'caseConference')

    GROUP BY patientId

),

current_year_appts as (

    select patientId
    from {{ ref('member_appointment_statuses') }}
    where derivedAppointmentStatus = 'past_appointment'
    group by patientId
    having extract(year from max(startAt)) = extract(year from current_date)

),

questions_answers_all as (
    select patientId,
           safe_cast(answerValue as timestamp) as answerValue,
           questionSlug,
           assessmentSlug,
           patientAnswerCreatedAt

    from {{ ref('questions_answers_all') }}

),

hra_assessment as (
    select
         patientId,
         min(answerValue) as earliestHraCompletedAt,
         max(answerValue) as latestHraCompletedAt

    from questions_answers_all
    where
    -- with Question Slug as 'assessment-reference-date-time' , it just shows the timestamp date for HRA assessment. It doesnot determine the HRA is ready for review ( for CBH).
     questionSlug = 'assessment-reference-date-time' and
     assessmentSlug = 'health-risk-assessment-ma'
    group by patientId
),

comp_assessments as (
--Submission Dates of Comp
    select 
      patientId,
      min(scoredAt) as minComprehensiveAssessmentAt,
      max(scoredAt) as maxComprehensiveAssessmentAt
    from {{ source('commons', 'patient_screening_tool_submission') }} 
    where 
      screeningToolSlug in ('comprehensive', 'baseline-nyc-ct','comprehensive-dc') and
      scoredAt is not null
    group by patientId

),

mds_assessments as (
-- submission Dates of MDS
    select 
      patientId,
      min(scoredAt) as minMdsAssessmentAt,
      max(scoredAt) as maxMdsAssessmentAt
    from {{ source('commons', 'patient_screening_tool_submission') }} 
    where 
      screeningToolSlug = 'minimum-data-set' and
      scoredAt is not null
    group by patientId

),

mds_interviews as (
-- Ready for CBH's Review MDS Dates
    select
      patientId,
      min(patientAnswerCreatedAt) as earliestMdsInterviewCompletedAt,
      max(patientAnswerCreatedAt) as latestMdsInterviewCompletedAt

    from questions_answers_all

    where
      questionSlug = 'ready-for-review' and
      assessmentSlug = 'minimum-data-set'

    group by 1

),



min_initial_assessments as (

    select
      et.patientId,
      greatest(et.minEssentialsCompletedAt, md.minDemographicsAt) as minInitialAssessmentAt

    from {{ ref('essential_tools') }} et

    inner join min_demographics md
    using (patientId)

    where et.minEssentialsCompletedAt is not null

),

patient_document_types as (

    select
      patientId,
      hasPlanOfCareAttestation,
      earliestPlanOfCareAttestationAt,
      latestPlanOfCareAttestationAt

    from {{ ref('patient_document_types') }}

),

member_commons_completion as (

    select
        p.id as patientId,
        hcp_molst.hasHealthcareProxy,
        hcp_molst.hasMolst,
        hcp_molst.hasAdvanceDirectives,
        min_acp_document_upload.minAcpDocumentUploadAt,
        md.minDemographicsAt,
        has_assessment.patientId is not null as hasAssessment,
        has_any_answers.patientId is not null as hasAnyAnswers,
        care_plan.minTaskAt,
        care_plan.hasGeneratedCarePlan,
        care_plan.minCompletedTaskAt,
        care_plan.maxCompletedTaskAt,
        care_plan.hasCompletedOneTask,
        care_plan.maxGoalUpdatedAt,
        ma.acuityAssignedAt,
        acc.minCaseConferenceAt,
        acc.maxCaseConferenceAt,
        cast(floor(timestamp_diff(current_timestamp(), acc.maxCaseConferenceAt, hour)/24.0) AS INT64) AS daysSinceLastCaseConference,
        acc.totalNumCaseConferenceEver,
        vml.verifiedMedListAt,
        cya.patientId is not null as hasHadAppointmentThisYear,
        coalesce(ca.minComprehensiveAssessmentAt,hra.earliestHraCompletedAt) as minComprehensiveAssessmentAt,
        coalesce(hra.latestHraCompletedAt,ca.maxComprehensiveAssessmentAt) as maxComprehensiveAssessmentAt,
        coalesce(mds.minMdsAssessmentAt, hra.earliestHraCompletedAt) as minMdsAssessmentAt,
        coalesce(hra.latestHraCompletedAt, mds.maxMdsAssessmentAt) as maxMdsAssessmentAt,
        --previous logic
        coalesce(least(coalesce(ca.minComprehensiveAssessmentAt, mi.earliestMdsInterviewCompletedAt) ,coalesce(mi.earliestMdsInterviewCompletedAt,ca.minComprehensiveAssessmentAt)),hra.earliestHraCompletedAt) as bothTuftsContractualAssessmentsAt,
        --new logic + 365 for new members starting June or Nov 2021
        --coalesce(hra.latestHraCompletedAt, (least(ca.maxComprehensiveAssessmentAt, mi.latestMdsInterviewCompletedAt))) as latestTuftsContractualAssessmentsAt,
        coalesce(mi.earliestMdsInterviewCompletedAt, hra.earliestHraCompletedAt) as earliestMdsInterviewCompletedAt,
        coalesce(hra.latestHraCompletedAt,mi.latestMdsInterviewCompletedAt) as latestMdsInterviewCompletedAt,
        hra.earliestHraCompletedAt,
        hra.latestHraCompletedAt,
        mia.minInitialAssessmentAt,
        pdt.hasPlanOfCareAttestation,
        pdt.earliestPlanOfCareAttestationAt,
        pdt.latestPlanOfCareAttestationAt

    from {{ source('commons', 'patient') }} as p

    left join hcp_molst
        on p.id = hcp_molst.patientId
    
    left join min_acp_document_upload
        on p.id = min_acp_document_upload.patientId

    left join min_demographics as md
        on p.id = md.patientId

    left join has_assessment
        on p.id = has_assessment.patientId

    left join has_any_answers
        on p.id = has_any_answers.patientId

    left join care_plan
        on p.id = care_plan.patientId

    left join min_acuity as ma
        on p.id = ma.patientId

    left join agg_case_conference as acc
        on p.id = acc.patientId

    left join {{ ref('verified_med_list') }} as vml
        on p.id = vml.patientId

    left join current_year_appts as cya
        on p.id = cya.patientId

    left join hra_assessment as hra
        on p.id = hra.patientId

    left join comp_assessments as ca
        on p.id = ca.patientId

    left join mds_assessments as mds
        on p.id = mds.patientId

    left join mds_interviews as mi
        on p.id = mi.patientId

    left join min_initial_assessments as mia
        on p.id = mia.patientId

    left join patient_document_types as pdt
        on p.id = pdt.patientId

)

select *,
-- needed for MemberEngagaement Metric
      greatest(coalesce(maxMdsAssessmentAt,maxComprehensiveAssessmentAt),coalesce(maxComprehensiveAssessmentAt,maxMdsAssessmentAt))  as latestOfBothMdsComp,
      least(coalesce(minMdsAssessmentAt,minComprehensiveAssessmentAt),coalesce(minComprehensiveAssessmentAt,minMdsAssessmentAt)) as earliestOfBothMdsComp
 from member_commons_completion
