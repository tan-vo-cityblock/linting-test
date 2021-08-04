-- health home member assessment conditions
-- author: nabig chaudhry

-- cte 1: obtaining member health home enrollment and status info
with hh_members as (
  select patientId
  from {{ref( 'health_home_tracking_report' )}} ),
  
-- cte 2: obtaining health home specific member conditions from questions answered in assessment
assessment_conditions as (
  select patientId,
  submissionCompletedAt as assessmentCompletedAt,
  case 
    when isLatestPatientAnswer = true and questionSlug = 'where-have-you-lived-most-in-the-past-2-months' 
    and answerSlug = 'homeless-shelter' then 'HUD 1: Literally Homeless'
    when isLatestPatientAnswer = true and questionSlug = 'where-have-you-lived-most-in-the-past-2-months' 
    and answerSlug = 'anywhere-outside' then 'HUD 1: Literally Homeless'
    when isLatestPatientAnswer = true and questionSlug = 'where-have-you-lived-most-in-the-past-2-months' 
    and answerSlug = 'staying-with-family-or-friends' then 'HUD 2: Imminent Risk of Homelessness' 
    when isLatestPatientAnswer = true and questionSlug = 'where-have-you-lived-most-in-the-past-2-months' 
    and answerSlug = 'hospital' then 'HUD 2: Imminent Risk of Homelessness' 
    when isLatestPatientAnswer = true and questionSlug = 'where-have-you-lived-most-in-the-past-2-months' 
    and answerSlug = 'hotel-motel' then 'HUD 2: Imminent Risk of Homelessness'
    when isLatestPatientAnswer = true and questionSlug = 'where-have-you-lived-most-in-the-past-2-months' 
    and answerSlug = 'apt-house-room-govnt-subsidy' then 'No HUD Code: Housed'
    when isLatestPatientAnswer = true and questionSlug = 'where-have-you-lived-most-in-the-past-2-months' 
    and answerSlug = 'apt-house-room-no-govnt-subsidy' then 'No HUD Code: Housed'
    else 'N/A'
    end as healthHomeAssessmentHousingCurrent,
  case
    when isLatestPatientAnswer = false and questionSlug = 'where-have-you-lived-most-in-the-past-2-months' 
    and answerSlug = 'homeless-shelter' then 'HUD 1: Literally Homeless'
    when isLatestPatientAnswer = false and questionSlug = 'where-have-you-lived-most-in-the-past-2-months' 
    and answerSlug = 'anywhere-outside' then 'HUD 1: Literally Homeless'
    when isLatestPatientAnswer = false and questionSlug = 'where-have-you-lived-most-in-the-past-2-months' 
    and answerSlug = 'staying-with-family-or-friends' then 'HUD 2: Imminent Risk of Homelessness' 
    when isLatestPatientAnswer = false and questionSlug = 'where-have-you-lived-most-in-the-past-2-months' 
    and answerSlug = 'hospital' then 'HUD 2: Imminent Risk of Homelessness' 
    when isLatestPatientAnswer = false and questionSlug = 'where-have-you-lived-most-in-the-past-2-months' 
    and answerSlug = 'hotel-motel' then 'HUD 2: Imminent Risk of Homelessness'
    when isLatestPatientAnswer = false and questionSlug = 'where-have-you-lived-most-in-the-past-2-months' 
    and answerSlug = 'apt-house-room-govnt-subsidy' then 'No HUD Code: Housed'
    when isLatestPatientAnswer = false and questionSlug = 'where-have-you-lived-most-in-the-past-2-months' 
    and answerSlug = 'apt-house-room-no-govnt-subsidy' then 'No HUD Code: Housed'
    else 'N/A'
    end as healthHomeAssessmentHousingPast
  from {{ref('questions_answers_all')}} ),

-- cte 3: joining together and creating health home member conditions table
final as (
 select h.patientId,
  assessmentCompletedAt,
  healthHomeAssessmentHousingCurrent,
  healthHomeAssessmentHousingPast
  from hh_members h
  inner join assessment_conditions a
  on h.patientId = a.patientId 
  group by h.patientId, assessmentCompletedAt, healthHomeAssessmentHousingCurrent, healthHomeAssessmentHousingPast)

-- final query
select * from final