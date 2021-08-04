with info as (
  select patientId, 
  patientName
  from `cityblock-analytics.mrt_commons.member` ),
  

patientAnswer as (
    select * from
 `cbh-db-mirror-prod.commons_mirror.patient_answer` ),

patientScreeningToolSubmission as (
    select * from
  `cbh-db-mirror-prod.commons_mirror.patient_screening_tool_submission` ),

assessmentcompletionMA as (

  select p.patientId,
        date(p.answerValue)  as assessmentCompletionDate
  from patientAnswer p
  inner join patientScreeningToolSubmission s
    on s.id = p.patientScreeningToolSubmissionId
  where questionSlug = "assessment-reference-date-time" and s.screeningToolSlug = "health-risk-assessment-ma"
),

answer as (
select
  p.patientId,
  s.userId,
  p.createdAt,
  p.updatedAt,
  case
    when p.updatedAt is not null then 'Yes'
    else 'No'
    end as questionUpdated,
  p.deletedAt,
  case
    when p.deletedAt is not null then 'Yes'
    else 'No'
    end as questionDeleted,
  p.questionText,
  p.answerText,
  s.screeningToolSlug
  from patientAnswer p
  inner join patientScreeningToolSubmission s
  on s.id = p.patientScreeningToolSubmissionId

  where s.screeningToolSlug in ("comprehensive", "baseline-nyc-ct","comprehensive-dc", "minimum-data-set", "long-term-services-coordinator-screening-tool","health-risk-assessment-ma", "rating-category-reporting")
  --CF DC
  -- ('comprehensive', 'baseline-nyc-ct','comprehensive-dc')
  --where baselineAssessmentCompletionId is not null  is commented out only for MA tufts market
  --where baselineAssessmentCompletionId is not null
  ),

baseline as (
  select patientId,
  completedAt
  from `cbh-db-mirror-prod.commons_mirror.baseline_assessment_completion`
  ----where completedAt is not null is commented out only for MA tufts market.
  --where completedAt is not null
  ),

user as (
  select id,
  userRole,
  concat(firstName,' ', lastName) as userName
  from `cbh-db-mirror-prod.commons_mirror.user` ),

final as (
  select i.patientId as memberId,
  i.patientName as memberName,
  greatest(coalesce(max(date(b.completedAt)),max(assessmentCompletionDate)), coalesce(max(assessmentCompletionDate),max(date(b.completedAt)))) as baselineAssessmentCompletedDate,
  max(a.createdAt) as baselineQuestionDate,
  a.questionText as question,
  max(a.answerText) as answer,
  --max(a.questionUpdated) as baselineQuestionUpdated,
  --max(a.updatedAt) as dateQuestionUpdated,
  --max(a.questionDeleted) as baselineQuestionDeleted,
  --max(a.deletedAt) as dateQuestionDeleted,
  u.userRole as roleAssessmentSubmittedBy,
  u.userName as roleAssessmentSubmittedByName,
  a.screeningToolSlug as toolName
  from info i
  inner join answer a
  on i.patientId = a.patientId
  inner join baseline b
  on i.patientId = b.patientId
  inner join user u
  on a.userId = u.id
  left join assessmentcompletionMA m
  on i.patientId = m.patientId
  group by i.patientId,i.patientName,a.questionText, u.userRole, u.userName, a.screeningToolSlug)

select * from final
where roleAssessmentSubmittedBy not in ("Community_Health_Partner")
