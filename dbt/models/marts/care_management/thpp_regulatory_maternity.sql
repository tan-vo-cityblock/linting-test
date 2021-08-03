with members as (

  select *
  from {{ ref('member') }}
),

latest_completed_questions_answers_current as (

  select *
  from {{ ref('questions_answers_current') }}
  where submissionDeletedAt is null
        and isLatestPatientAnswer is true
        and submissionCompletedAt is not null

),

pregnancy_info as (
  
  select distinct
    m.patientId,
    m.firstName,
    m.lastName,
    m.nmi as memberId
    
  from members m
  inner join latest_completed_questions_answers_current qa
  using (patientId)
  
  where (m.patientHomeMarketName = 'Massachusetts') and
    (qa.questionSlug = 'are-you-currently-pregnant' and
     qa.answerSlug = 'yes' and 
     date_diff(current_date, date(qa.patientAnswerCreatedAt), month) <= 9)
), 

due_date_info as (
  
  select distinct
    patientId,
    answerText as pregnancyDueDate
  
  from latest_completed_questions_answers_current
  
  where questionSlug = 'when-is-your-estimated-due-date'
),

number_of_babies_info as (

  select distinct
    patientId,
    answerText as pregnancyNumberOfBabiesExpected,
    
  from latest_completed_questions_answers_current
  
  where questionSlug = 'are-you-pregnant-with-two-or-more-babies'
  
), 

final as (
  
  select
   	firstName,
    lastName,
    memberId,
    'Yes' as pregnancyStatus,
    'Yes' as pregnancyReportConsentStatus,
    coalesce(pregnancyDueDate,'Unknown') as pregnancyDueDate,
    coalesce(pregnancyNumberOfBabiesExpected,'Unknown') as pregnancyNumberOfBabiesExpected,
    
    from pregnancy_info
    left join due_date_info using (patientId)
    left join number_of_babies_info using (patientId)
    
)

select * from final
