
with screening_tool_answers as (

  select
    submissionId as followUpId,
    patientId,
    userId as followUpUserId,
    submissionCompletedAt as followUpAt,
    questionSlug,
    answerSlug,
    answerText
    
  from {{ ref('questions_answers_current') }}

  where 
    assessmentSlug = 'transitions-of-care-screening-tool' and 
    submissionCompletedAt is not null and 
    questionSlug in (

    'are-you-able-to-proceed-with-the-toc-tool', 
    'what-is-the-relationship-of-this-person-to-the-member-toc'

  )

),

followups as (

  select distinct
    followUpId,
    'patient_screening_tool_submission' as followUpSource,
    patientId,
    followUpUserId,
    followUpAt,
    'screeningTool' as followUpModality,
    true as isTocFollowUp,
    string(null) as followUpOutcome,
    'screening tool' as followUpType
    
  from screening_tool_answers 

),

successful_screenings as (

  select followUpId

  from screening_tool_answers

  where
    questionSlug = 'are-you-able-to-proceed-with-the-toc-tool' and
    answerSlug = 'yes'

),

reached_relationships as (

  select 
    followUpId,
    answerText as followUpRecipients

  from screening_tool_answers

  where questionSlug = 'what-is-the-relationship-of-this-person-to-the-member-toc'

),

final as (

  select
    f.*,
    
    case
      when ss.followUpId is not null
        then 'success'
      else 'attempt'
    end as followUpStatus,
    
    rr.followUpRecipients
    
  from followups f

  left join successful_screenings ss
  using (followUpId)

  left join reached_relationships rr
  using (followUpId)

)

select * from final
