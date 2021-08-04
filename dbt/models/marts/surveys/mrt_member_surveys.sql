
with base as (

  select 
    ams.id as responseId,
    patientId,
    communicationSent as communicationSentAt,
    created as responseCreatedAt,
    modified as responseModifiedAt,
    row_number() over(partition by ams.id order by modified desc) = 1 as isLatestResponseVersion,
    row_number() over(partition by patientId, isNpsQuestion order by modified desc) = 1 and isNPSQuestion is true as isLatestNPSResponseByMember,
    interaction.id as interactionId,
    split(interaction.locationCode,":")[safe_offset(0)] as interactionSetting,
    m.name as interactionMarket, 
    interaction.startTimestamp as interactionStartAt,
    interaction.endTimestamp as interactionEndAt,
    coalesce(u.id, u1.id) as userId,
    survey.id as surveyId,
    survey.name as surveyName,
    surveyOpened as surveyOpenedAt,
    surveySubmitted as surveySubmittedAt,
    surveyResponseId,
    question.id as questionId,
    question.text as questionText,
    case
      when question.type = 'FT' then 'Free text'
      when question.type = 'RT' then 'Rating (1-5)'
      when question.type = 'MC' then 'Multiple choice (single-select)'
      when question.type = 'MM' then 'Multiple choice (multi-select)'
      when question.type = 'NP' then 'Net Promoter Score (0-10)'
    end as questionType,
    isNpsQuestion,
    score as ratingScore,
    textAnswer as freeTextAnswer,
    choice as singleSelectOption,
    safe_cast(npsScore as int64) as npsScore,
    multiSelectOption
  from {{ ref('abs_member_surveys') }} ams
  left join unnest(interactors) as interactor
  left join unnest(choices) as multiSelectOption
  left join {{ source('commons', 'market') }} m
  on split(interaction.locationCode,":")[safe_offset(1)] = m.slug 
  left join {{ source('commons', 'user') }} u
  on interactor.clientInteractorId = regexp_replace(u.id, r"[-]", "")
  left join {{ source('commons', 'user') }} u1
  on lower(interactor.clientInteractorId) = lower(concat(u1.firstName, u1.lastName))

),

promoter_categories as (

  select * except (isLatestResponseVersion, multiSelectOption),
    case
      when npsScore is null then null
      when npsScore > 8 then 'Promoter'
      when npsScore > 6 then 'Neutral'
      else 'Detractor'
    end as promoterCategory,
    multiSelectOption
  from base
  where isLatestResponseVersion is true

),

final as (

  select * except (multiSelectOption),
    case
      when promoterCategory = 'Promoter' then 100
      when promoterCategory = 'Neutral' then 0
      when promoterCategory = 'Detractor' then -100
    end as promoterScore,
    multiSelectOption
  from promoter_categories 

)

select * from final
