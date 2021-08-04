with qna_1 as (

  select
      pa.patientId,
      pa.userId, 
      pa.questionSlug,
      pa.questionText,
      case when q.slug is null then true else false
      end questionSlugIsDeprecated, -- flagging deprecated question slugs
      pa.answerSlug,
      pa.answerText,
      pa.answerValue,
      coalesce(psts.id, raas.id) as assessmentId,
      coalesce(psts.screeningToolSlug, raas.assessmentSlug) as assessmentSlug,
      case when psts.screeningToolSlug is not null then 'screeningTool' else 'assessment' end as assessmentType, 
      ast.riskAreaGroupSlug,
      coalesce(ast.isAutomated, False) as isAutomated, -- NULL for screening tools which are not automated
      q.kind as answerType,
      q.isConditional as questionIsConditional,
      qast.questionOrder,
      pa.id as patientAnswerId,
      coalesce(pa.patientScreeningToolSubmissionId, pa.riskAreaAssessmentSubmissionId) as submissionId,
      coalesce(raas.createdAt, psts.createdAt) as submissionCreatedAt,
      coalesce(raas.completedAt, psts.scoredAt) as submissionCompletedAt,
      coalesce(psts.deletedAt, raas.deletedAt) as submissionDeletedAt,
      psts.score as screeningToolScore,
      pa.createdAt as patientAnswerCreatedAt, 
      pa.deletedAt as patientAnswerDeletedAt,
      qast.isAssociatedWithMds

    from {{ source('commons', 'patient_answer') }}  as pa
    
    left join {{ source('commons', 'risk_area_assessment_submission') }} as raas
        on pa.riskAreaAssessmentSubmissionId = raas.id
        
    left join {{ source('commons', 'patient_screening_tool_submission') }} as psts
        on pa.patientScreeningToolSubmissionId = psts.id

    left join {{ source('commons', 'assessment') }} as ast
        on raas.assessmentSlug = ast.slug

    left join {{ source('commons', 'builder_question') }} q
        on pa.questionSlug = q.slug

    left join {{ ref('src_builder_question_assessment_screening_tool') }}  qast
        on 
          coalesce(raas.assessmentSlug, psts.screeningToolSlug) = coalesce(qast.assessmentSlug, qast.screeningToolSlug) and
          pa.questionSlug = qast.questionSlug

    where 
      (psts.id is not null or raas.id is not null) and -- no progress notes
      pa.questionSlug is not null and
      (pa.answerSlug is not null or pa.answerText is not null)
      
),

qna_row_num as (
select
*,
--we want capture the latest completed answer regardless of the submission status
row_number() over (partition by patientid, assessmentSlug, questionSlug order by patientAnswerCreatedAt desc) as row_num
from qna_1
),

qna as (
select *,
--multiselect answer type should not be included in row number since there are multiple answers
--and we want capture the latest non-deleted submission and completed (non-deleted) answer
    case when answerType = 'multiselect' and patientAnswerDeletedAt is null
        and submissionDeletedAt is null
     then true
    when row_num = 1 and answerType <> 'multiselect' and patientAnswerDeletedAt is null
        and submissionDeletedAt is null
     then True
    else False
      end as isLatestPatientAnswer,
  --indicator to help identify if latestpatientanswer is a submitted assessment
   submissionCompletedAt is not null as issubmitted
from qna_row_num
),


qna_freetext as ( 
  -- Freetext historical and current answers should be processed identically to remove autosaves
  -- There are a examples of multiple nulls occuring in a minority of freetext question-submissions
  -- In these cases we can't trust the Commons DB constraints to have a single non-null deleted entry

  select 
    qna.*,
    row_number() over (
            partition by patientId, submissionId, questionSlug order by patientAnswerCreatedAt desc) as patientQuestionSubmissionReverseIteration
  from qna
  where answerType = 'freetext'
),

qna_not_freetext as (

  select
    *
  from qna
  where answerType <> 'freetext'
),

qna_current as (

  select 
    qna_not_freetext.*,
    1 as patientQuestionSubmissionReverseIteration
  from qna_not_freetext
  where isLatestPatientAnswer = True
),

-- Anti-join to find submission-questions where all answers are deleted
qna_not_current as (

  select 
    qna_not_freetext.* 
  from qna_not_freetext
  left join qna_current
  on qna_not_freetext.patientId = qna_current.patientId and qna_not_freetext.submissionId = qna_current.submissionId and qna_not_freetext.questionSlug = qna_current.questionSlug
  where qna_current.questionSlug is null
),

qna_historic as (

  select
    qna_not_current.*,
    case
        -- Select latest answer if we only expect one
        when answerType <> 'multiselect'  then
          row_number() over (
            partition by patientId, submissionId, questionSlug order by patientAnswerCreatedAt desc)
        -- At best for multiselect answers can select distinct set of answers (latest within each question-submission)
        else 
          row_number() over (
            partition by patientId, submissionId, questionSlug, answerSlug order by patientAnswerCreatedAt desc) end as patientQuestionSubmissionReverseIteration -- Pick final answer in submission-question for non-multiselect answers
    from qna_not_current
),

qna_deprecated as (

  select
    *,
    1 as patientQuestionSubmissionReverseIteration
  from qna
  where questionSlugIsDeprecated = true

),

qna_union as (
  
  select * from qna_freetext where patientQuestionSubmissionReverseIteration = 1
  union all
  select * from qna_current where patientQuestionSubmissionReverseIteration = 1
  union all
  select * from qna_historic where patientQuestionSubmissionReverseIteration = 1
  union all
  select * from qna_deprecated where patientQuestionSubmissionReverseIteration = 1
)

select
  patientId,
  userId,
  questionSlug,
  questionText,
  questionSlugIsDeprecated,
  answerSlug,
  answerText,
  answerValue,
  assessmentId,
  assessmentSlug,
  assessmentType, 
  riskAreaGroupSlug,
  isAutomated,
  answerType,
  questionIsConditional,
  questionOrder,
  patientAnswerId,
  submissionId,
  submissionCreatedAt,
  submissionCompletedAt,
  submissionDeletedAt,
  patientAnswerCreatedAt, 
  patientAnswerDeletedAt,
  screeningToolScore,
  isLatestPatientAnswer,
  isAssociatedWithMds,
  issubmitted,

  case 
    when answerType <> 'multiselect' then
      row_number() over (
        partition by patientId, assessmentSlug, questionSlug, timestamp_trunc(patientAnswerCreatedAt, DAY, 'America/New_York') order by patientAnswerCreatedAt desc)
    else
      row_number() over (
        partition by patientId, assessmentSlug, questionSlug, answerSlug, timestamp_trunc(patientAnswerCreatedAt, DAY, 'America/New_York') order by patientAnswerCreatedAt desc)
  end as patientQuestionAnswerDayReverseIteration
from qna_union
order by patientId, questionSlug
