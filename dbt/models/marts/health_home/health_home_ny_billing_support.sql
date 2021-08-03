with service_dates as (

  select distinct 
    patientId, 
    format_date("%m%d%Y",date(timestamp_trunc(eventTimestamp, month))) as serviceDate
  
  from  {{ ref( 'member_interactions' ) }} mi
  inner join {{ source('commons', 'patient_document') }} pd
  using (patientId)
  where mi.isSuccessfulTend is true and
  mi.eventType not in ('text', 'email', 'mail', 'fax') and
  pd.documentType in ('healthHomeInformationConsent', 'verbalHealthHomeInformationConsent') and
  pd.deletedAt is null and
  date(timestamp_trunc(mi.eventTimestamp, month)) >= date(timestamp_trunc(pd.createdAt, month)) 

),

computed_fields as (

  select
    patientId,
    
    concat(
    
      case when healthHomeComputedMentalHealth = true then '02' else '' end,
      case when healthHomeComputedSubstanceAbuse = true then '04' else '' end,
      case when healthHomeComputedAsthma = true then '06' else '' end,
      case when healthHomeComputedDiabetes = true then '08' else '' end,
      case when healthHomeComputedHeartDisease = true then '10' else '' end,
      case when healthHomeComputedOverweight = true then '12' else '' end,
      case when healthHomeComputedHiv = true then '14' else '' end,
      case when healthHomeComputedOther = true then '16' else '' end,
      case when healthHomeComputedComplexTrauma = true then '18' else '' end,
      case when healthHomeComputedSeriousMentalIllness = true then '20' else '' end,
      case when healthHomeComputedDevelopmentalDisabilities = true then '22' else '' end, 
      case when healthHomeComputedChildrenHCBSOnly = true then '24' else '' end,
      case when healthHomeComputedChildrenHCBSOther = true then '26' else '' end,
      case when healthHomeComputedAdultHCBSOther = true then '28' else '' end
      
    ) as preconditions,
      
    case
      when healthHomeComputedOtherDescription = 'kidney failure' then 'Kidney Failure'
      when healthHomeComputedOtherDescription = 'chronic pain' then 'Chronic Pain'
      when healthHomeComputedOtherDescription = 'dementia' then 'Dementia'
      when healthHomeComputedOtherDescription = 'cirrhosis' then 'Cirrhosis'
      when healthHomeComputedOtherDescription = 'muscular pain and disease' then 'Muscular Pain and Disease'
      when healthHomeComputedOtherDescription = 'liver inflammation and damage' then 'Liver Inflammation and Damage'
      when healthHomeComputedOtherDescription = 'hypertension' then 'Hypertension'
      when healthHomeComputedOtherDescription = 'cancer' then 'Cancer'
      when healthHomeComputedOtherDescription = 'stroke' then 'Stroke'   
      when healthHomeComputedOtherDescription = 'osteoporosis' then 'Osteoporosis'
      else repeat(' ', 40)
    end as preconditionsOtherDescription,
  
    case
      when healthHomeComputedHiv = true then 'Y' 
      else 'N' 
    end as hivStatus,

    case
      when healthHomeComputedHiv = true then '1' 
      else '0' 
    end as hivViralLoad,

    case
      when healthHomeComputedHiv = true then '1' 
      else '0' 
    end as hivTCellCount
  
  from {{ ref('health_home_cf_conditions') }}

),

housing as (

  select 
    patientId,
    max(
      case
        when healthHomeAssessmentHousingCurrent = 'HUD 1: Literally Homeless' or
        healthHomeAssessmentHousingCurrent = 'HUD 2: Imminent Risk of Homelessness' then 'Y'
        else 'N'
      end 
    ) as housingStatus,
    max(
      case
        when healthHomeAssessmentHousingCurrent = 'HUD 1: Literally Homeless' then '1'
        when healthHomeAssessmentHousingCurrent = 'HUD 2: Imminent Risk of Homelessness' then '2'
        else repeat(' ', 1)
      end 
    ) as housingHudCategory
  
  from {{ ref('health_home_asmt_conditions') }}
  group by patientId
   
),

homeless_past as (

  select  
    patientId,
    max(
    case
      when h.housingStatus = 'N' and
      a.healthHomeAssessmentHousingPast in ('HUD 1: Literally Homeless', 'HUD 2: Imminent Risk of Homelessness') then 'Y'
      when housingStatus = 'N' and 
      a.healthHomeAssessmentHousingPast in ('No HUD Code: Housed', 'N/A') then 'N'
      else repeat(' ', 1)
    end
    ) as hud1within6MonthsStatus,
    max(
    case 
      when a.healthHomeAssessmentHousingPast in ('HUD 1: Literally Homeless', 'HUD 2: Imminent Risk of Homelessness') then format_date('%m%d%Y', cast(a.assessmentCompletedAt as date))
      else repeat(' ', 1)
      end
    ) as assessmentDate
  
  from housing h
  inner join {{ ref('health_home_asmt_conditions') }} a
  using (patientId) 
  where 
    date(a.assessmentCompletedAt) >= date_sub(current_date, interval 6 month)
  group by patientId
),

hie_events as (

  select
    patient.patientId,
    format_date('%m%d%Y', date(eventDateTime.instant)) as eventDate,
    eventType,
    visitType,
    lower(diagnoses.description) as diagnosesDescription
  
  from {{ ref('src_patient_hie_events') }}
  left join unnest(diagnoses) as diagnoses
  where date_diff(current_date('America/New_York'), date(eventDateTime.instant), month) <= 12

),

claims_events as (

  select
    patientId,
    format_date('%m%d%Y', dateTo) as eventDate,
    serviceCategory,
    serviceSubCategory,
    costCategory,
    costSubCategory,
    costSubCategoryDetail
    
  from {{ ref('mrt_claims_self_service') }} 
  where date_diff(current_date('America/New_York'), dateTo, month) <= 12 and
  partner = 'emblem'

),

events_incarcerations as (

  select 
    patientId, 
    'Y' as incarcerationStatus

  from hie_events
  where eventType = 'Incarceration'
  group by patientId

),

events_mental_illness as (

  select 
    ce.patientId,
    'Y' as mentalIllnessStatus,
    coalesce(max(ce.eventDate), max(he.eventDate)) as  mentalIllnessDischargeDate
    
  from claims_events ce
  left join hie_events he
  using (patientId)
  where
    (
    
    ce.serviceCategory = 'inpatientPsych' or
    
      (
    
      ce.costCategory = 'inpatient' and
      ce.costSubCategory in ('behavioralHealth','inpatientPsych','psych','rehab') and
      ce.costSubCategoryDetail != 'substanceAbuse'
    
      )
    
    )
    
    or
    
    (
    
    he.eventType = 'Discharge' and 
    he.visitType in ('IA', 'Inpatient') and
   
     (

        he.diagnosesDescription like '%behavioral%' or
        he.diagnosesDescription like '%mental%' or
        he.diagnosesDescription like '%psy%' or
        he.diagnosesDescription like '%anxiety%' or
        he.diagnosesDescription like '%therapy%' or
        he.diagnosesDescription like '%ptsd%' or
        he.diagnosesDescription like '%trauma%' or
        he.diagnosesDescription like '%schizophrenia%' or
        he.diagnosesDescription like '%personality%' or
        he.diagnosesDescription like '%depression%' or
        he.diagnosesDescription like '%bipolar%' or
        he.diagnosesDescription like '%eating%'

      )
    
    )
    
  group by patientId

),

events_substance_abuse as (

  select 
    ce.patientId,
    'Y' as substanceAbuseStatus,
    coalesce(max(ce.eventDate), max(he.eventDate)) as substanceAbuseDischargeDate
  
  from claims_events ce
  left join hie_events he 
  using (patientId)
  where
    
    (
    
    ce.serviceSubCategory = 'substanceAbuse' or
    ce.costSubCategoryDetail = 'substanceAbuse'
    
    )
    
    or
    
    (
    
    he.eventType = 'Discharge' and 
    he.visitType in ('IA', 'Inpatient') and
   
      (

       he.diagnosesDescription like '%substance%' or
       he.diagnosesDescription like '%drug%' or
       he.diagnosesDescription like '%opiates%' or
       he.diagnosesDescription like '%alcohol%' or
       he.diagnosesDescription like '%opioids%'

      )
    
    )

  group by patientId

),

screening_tool_results as (

  select
    psts.patientId,
    st.title,
    psts.score
    
  from  {{ source('commons', 'patient_screening_tool_submission') }} psts
  inner join  {{ source('commons', 'builder_screening_tool') }} st
  on 
    psts.screeningToolSlug = st.slug and
    st.slug in ('alcohol-abuse-screening-audit', 'drug-abuse-screening-dast')
  where 
    date(psts.createdAt) >= date_sub(current_date, interval 6 month) and
    psts.deletedAt is null and
    psts.scoredAt is not null

),

audit as (

  select 
    patientId, 
    'Y' as sudActiveUseStatus
  
  from screening_tool_results 
  where 
    title = 'Alcohol Abuse Screening (AUDIT)' and
    score >= 8
  group by patientId

),

dast as (

  select 
    patientId, 
    'Y' as sudActiveUseStatus
  
  from screening_tool_results 
  where 
    title = 'Drug Abuse Screening (DAST)' and
    score >= 3
  group by patientId

),

sud_active as (

  select * from audit
  union distinct
  select * from dast

),

final as (
  
  select
    h.patientId,
    h.fullName,
    'A' as addVoidIndicator,
    h.medicaidId,
    s.serviceDate,
    repeat(' ', 10) as diagnosisCode,
    c.preconditions,
    c.preconditionsOtherDescription,
    c.hivStatus,
    c.hivViralLoad,
    c.hivTCellCount,
    
    ho.housingStatus,
    ho.housingHudCategory,
    
    coalesce(ei.incarcerationStatus, 'N') as incarcerationStatus,
    repeat(' ', 8) as incarcerationReleaseDate,
    
    coalesce(emi.mentalIllnessStatus, 'N') as mentalIllnessStatus,
    coalesce(emi.mentalIllnessDischargeDate, repeat(' ', 8)) as mentalIllnessDischargeDate,
    
    coalesce(esa.substanceAbuseStatus, 'N') as substanceAbuseStatus,
    coalesce(esa.substanceAbuseDischargeDate, repeat(' ', 8)) as substanceAbuseDischargeDate,
    coalesce(sa.sudActiveUseStatus, 'N') as sudActiveUseStatus,
    
    'Y' as coreServicesProvidedStatus,
    'N' as aotStatus,
    repeat(' ', 1) as aotMinimumServicesProvidedStatus,
    'N' as actStatus,
    repeat(' ', 1) as actMinimumServicesProvidedStatus,
    'N' as ahAdultHomePlusQualificationStatus,
    repeat(' ', 1) as ahCommunityTransitionStatus,
    repeat(' ', 1) as ahContinuedQualificationStatus,
    repeat(' ', 1) as ahTransitionInterestStatus,
    'N' as cmaDirectBillerIndicatorStatus,
    'N' as childFosterCareStatus,
    hud1within6MonthsStatus,
    
    case
      when hp.hud1within6MonthsStatus = 'Y' and ho.housingStatus = 'N' 
      then 'Y'
      when hp.hud1within6MonthsStatus = 'Y' and ho.housingStatus = 'Y' 
      then 'N'
      else repeat(' ', 1)
    end as pastHud1HousedStatus,
      
    case
      when hp.hud1within6MonthsStatus = 'Y' and ho.housingStatus = 'N'
      then hp.assessmentDate
      else repeat(' ', 1)
    end as pastHud1HousedDate,

    'A' as expandedHhPopulation,
    repeat (' ', 1) as hhPlusMinimumServicesProvidedStatus,
    'N' uasComplexAssessmentStatus
  
  from {{ ref('health_home_tracking_report') }} h
  left join service_dates s
  using (patientId)
  left join computed_fields c
  using (patientId)
  left join housing ho
  using (patientId)
  left join events_incarcerations ei
  using (patientId)
  left join events_mental_illness emi
  using (patientId)
  left join events_substance_abuse esa
  using (patientId)
  left join sud_active sa
  using (patientId)
  left join homeless_past as hp
  using (patientId)

)

select * from final