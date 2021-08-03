with patient_answer as (

  select
    patientId,
    patientScreeningToolSubmissionId,
    createdAt
  from {{ source('commons', 'patient_answer') }}
),

patient_screening_tool_submission_scored as (

  select
    id,
    screeningToolSlug,
    scoredAt
  from {{ source('commons', 'patient_screening_tool_submission') }}
  where scoredAt is not null
    and screeningToolSlug in ('baseline-nyc-ct',
                              'comprehensive', 
                              'depression-screening-phq-9',
                              'anxiety-screening-gad-7', 
                              'alcohol-abuse-screening-audit',
                              'drug-abuse-screening-dast',
                              'comprehensive-dc')
),

tools as (

  select 
    pa.patientId,
    pstss.screeningToolSlug,
    min(pa.createdAt) as answerAt,
    min(pstss.scoredAt) as scoredAt

  from patient_answer pa
  
  inner join patient_screening_tool_submission_scored pstss
  on pa.patientScreeningToolSubmissionId = pstss.id
  
  group by patientId, screeningToolSlug

),

earliest_answers as (

  select
    patientId,
    min(answerAt) as minEssentialsStartedAt
  from tools
  group by patientId

),

completed_essentials as (

  select 
    patientId, 
    array_agg(screeningToolSlug) as screeningToolSlugs
  from tools
  group by patientId
  having count(*) >= 5

),

completion_times as (

  select
    t.patientId,
    max(t.scoredAt) as minEssentialsCompletedAt

  from tools t

  inner join completed_essentials ce
    using (patientId)

  where not (

    array_length(ce.screeningToolSlugs) = 5 and
    'baseline-nyc-ct' in unnest(ce.screeningToolSlugs) and
    'comprehensive' in unnest(ce.screeningToolSlugs) 

  )

  group by t.patientId
  
),

final as (

  select 
    ea.patientId,
    ea.minEssentialsStartedAt,
    ct.minEssentialsCompletedAt 

  from earliest_answers ea

  left join completion_times ct
    using (patientId)

)

select * from final
