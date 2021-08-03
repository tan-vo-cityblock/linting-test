with member_info as (
select distinct
    m.patientId,
    mdi.externalId as memberId,
    upper(m.firstName) as firstName,
    upper(m.lastName) as lastName,
    current_date as reportRunDate,
    m.currentState as cityblockMemberState,
    date(ps.createdAt) as cityblockMemberStateStartDate,
    
    case
      when tsmi.cbHardToReachStatus is true then 'Y'
      else 'N'
    end as hardToReachOrEngageStatus,
    
    case
      when tsmi.cbHardToReachStatus is true then 'Member unresponsive' 
      else null 
    end as hardToReachOrEngageReason,
    
    case 
      when mcc.minComprehensiveAssessmentAt is not null then 'Y'
      else 'N'
    end as comprehensiveAssessmentCompleted,
    
    date(mcc.minComprehensiveAssessmentAt) as comprehensiveAssessmentCompletedDate,
    
    case 
      when earliestMdsInterviewCompletedAt is not null then 'Y'
      else 'N'
    end as mdsAssessmentCompleted,
    
    date(mcc.earliestMdsInterviewCompletedAt) as mdsAssessmentCompletedDate,
    date(mcc.bothTuftsContractualAssessmentsAt) as bothTuftsContractualAssessmentsCompletedDate,

    case
      when tsmi.cbMemberCarePlanRefusalDate is not null then 'Y'
      else 'N'
    end as carePlanRefusedFlag,

    case
      when tsmi.cbMemberCarePlanRefusalDate is null and mcc.minTaskAt is not null then 'Y'
      else 'N'
    end as carePlanDiscussedAndAgreedStatus,

    case
      when tsmi.cbMemberCarePlanRefusalDate is null and mcc.minTaskAt is not null then date(mcc.minTaskAt)
      else null
    end as carePlanDiscussedAndAgreedDate,
    
    case 
      when m.partnerAttributedPcpNpi is not null then 'Y'
      else 'N'
    end as doYouHaveAPcpYouGoToForYourHealthNeeds,
   
  from {{ ref('member') }} m
  left join {{ ref('src_member_insurance') }} mdi
  on
    m.patientId = mdi.memberId and
    mdi.datasourceId not in (28, 29)
    --and mdi.current = true
  left join {{ ref('thpp_sla_member_info') }} tsmi
  using (patientId)
  left join {{ ref('member_commons_completion') }} mcc
  using (patientId)
  left join {{source('commons', 'patient_state')}} ps
  on m.patientId = ps.patientId and m.currentState = ps.currentState
  
  where (m.patientHomeMarketName = 'Massachusetts') and
  ps.deletedAt is null
  
), 

latest_goals as (
  select *, 
  
  case
      when row_number() over(partition by patientId order by goalCreatedAt desc) = 1 then true
      else false
    end as latestGoal
    
  from {{ ref('member_goals_tasks') }}
  
),

goal_intervention as (
  select
    patientId,
    goalTitle as latestGoalDescription,

    case
      when goalTitle is not null and goalCompletedAt is null then 'Active'
      when goalTitle is not null and goalCompletedAt is not null then 'Inactive'
      else null
    end as latestGoalStatus,

    date(goalCreatedAt) as latestGoalCreatedDate,
    date(goalUpdatedAt) as latestGoalUpdateDate,
    goalDueAt as latestGoalTargetDate,
    date(goalCompletedAt) as latestGoalDateMet,
    date(taskCreatedAt) as latestInterventionUpdateDate    

  from latest_goals
  where latestGoal is true

),

ranked_deleted_goal_dates as (

  select
    id as goalId,
    patientId,
    date(deletedAt, "America/New_York") as latestGoalDateUnmetAndDeleted,
    rank() over(partition by patientId order by deletedAt desc, id) as rnk

  from {{ source('commons','goal') }}

  where deletedAt is not null

),

ranked_goal_comments as (

  select
    goalId,
    text as latestGoalUnmetReason,
    rank() over(partition by goalId order by createdAt desc, id) as rnk
  
  from {{ source('commons','goal_comment') }}

),

latest_deleted_goal_dates_with_comments as (

  select
    g.patientId,
    g.latestGoalDateUnmetAndDeleted,
    c.latestGoalUnmetReason
  
  from ranked_deleted_goal_dates g
  
  left join ranked_goal_comments c
  on
    g.goalId = c.goalId and
    c.rnk = 1
  
  where g.rnk = 1

),

care_team as (
  select
    pct.patientId,
    max(upper(coalesce(pct.primaryCommunityHealthPartnerName,pct.primaryNurseCareManagerName)))	as assignedCareManager,
  
    max(case
      when ect.providerRole = 'longTermServicesAndSupportsCoordinator' then upper(ect.providerName)
      else null
    end) as currentLtsc,
       
    max(case
      when ect.providerRole = 'longTermServicesAndSupportsCoordinator' then 'Y'
      else 'N'
    end) as isLtscPartOfIct
    
   
  from {{ ref('member_primary_care_team') }} pct
  left join {{ ref('member_external_care_team') }} ect
  using (patientId)
  
  group by patientId
  
),

ltss_evaluation as (
  select 
    patientId,
    'Y' as evaluatedByLtscStatus

  from {{ source('commons','patient_document') }} 
  
  where (otherFreeText in ('LTSC re-assessment','LTSS Assessment','LTSS Initial Assessment','LTSC ReAssessment') 
  or lower(filename) like '%ltss%') and 
  filename not in ("Dot's LTSS referral form.docx","Dot's LTSS referral form.pdf","LTSS_01312020.pdf") and
  deletedAt is null
  
  group by patientId
  
),

ltss_assessment as (
  select
    m.patientId,
    coalesce(l.evaluatedByLtscStatus, 'N') as evaluatedByLtscStatus

  from member_info m
  left join ltss_evaluation l
  using (patientId)

),

ltss_offer_dates as (

  select
    patientId,
    'Y' as ltssAssessmentbyLtscOffered,
    date(patientAnswerCreatedAt, "America/New_York") as ltssOfferDate,

    case
      when answerSlug = 'yes'
        then 'Y'
        else 'N'
    end as ltssAssessmentbyLtscAccepted,

    null as ltssAssessmentbyLtscNotAcceptedReason,
    row_number() over(partition by patientId order by patientAnswerCreatedAt, patientAnswerId) as rnk

  from {{ ref('questions_answers_all') }}

  where questionSlug = 'would-you-like-a-long-term-services-coordinator'

),

first_ltss_offers as (
  
  select * except (rnk)
  
  from ltss_offer_dates 
  
  where rnk = 1

),

transition_of_care as (
  select 
    m.patientId,
    max(case
      when psts.screeningToolSlug in ('discharge-tool','transitions-of-care-screening-tool') then date(psts.createdAt)
      else null
    end) as transitionOfCareAssessmentCompletedDate
  
  from member_info m
  left join {{ source('commons', 'patient_screening_tool_submission') }} psts
  on m.patientId = psts.patientId and psts.deletedAt is null

  group by m.patientId

),

assessment_questions as (
  select
    patientId,
    
    max(case
      when questionSlug = 'are-you-worried-that-you-may-not-have-stable-housing-in-the-next-two-months' and answerSlug = 'no' then 'Y'
      when questionSlug = 'are-you-worried-that-you-may-not-have-stable-housing-in-the-next-two-months' and answerSlug = 'yes' then 'N'
      else null
    end) as DoYouConsiderYourCurrentHousingToBeAStableLivingArrangement,
      
    max(case
      when (questionSlug = 'are-you-worried-that-you-may-not-have-stable-housing-in-the-next-two-months' and answerSlug = 'yes') and
        (questionSlug = 'where-have-you-lived-most-in-the-past-2-months' and 
        answerSlug in ('hotel-motel','anywhere-outside','homeless-shelter','hospital'))
        then 'Y'
      else 'N'
    end) as ifNoDoYouConsiderYourselfHomeless,
    
    max(case
      when questionSlug = 'are-there-caregivers-involved-in-your-care-or-in-place-to-help-you' and answerSlug = 'yes' then 'Y'
      when questionSlug = 'are-there-caregivers-involved-in-your-care-or-in-place-to-help-you' and answerSlug = 'no' then 'N'
      else null
    end) as areYouReceivingAnyPaidHomecareServicesPresently,
    
    max(case
      when questionSlug = 'do-you-have-any-difficulties-with-your-vision' and answerSlug = 'yes' then 'Y'
      when questionSlug = 'do-you-have-any-difficulties-with-your-vision' and answerSlug = 'no' then 'N'
      else null
    end) as massachusettsCommissionForTheBlind,
    
    max(case
      when questionSlug = 'are-you-currently-using-tobacco-products' and answerSlug = 'yes' then 'Y'
      when questionSlug = 'are-you-currently-using-tobacco-products' and answerSlug = 'no' then 'N'
      else null
    end) as doYouSmokeCigarettes,
    
    max(case
      when questionSlug = 'do-you-drink-alcohol-at-all' and answerSlug = 'yes' then 'Y'
      when questionSlug = 'do-you-drink-alcohol-at-all' and answerSlug = 'no' then 'N'
      else null
    end) as doYouDrinkAlcoholicBeverages,
    
    max(case
      when questionSlug = 'what-is-your-preferred-written-language' and answerSlug != 'english' then 'Y'
      when questionSlug = 'what-is-your-preferred-written-language' and answerSlug = 'english' then 'N'
      else null
    end) as printedInADifferentLanguage,
    
    max(case
      when questionSlug = 'lack-of-transportation-problem' and answerSlug = 'yes' then 'Y'
      when questionSlug = 'lack-of-transportation-problem' and answerSlug = 'no' then 'N'
      else null
    end) as doYouNeedSupportToAssistYouWithTransportationToMedicalAppointments,
    
    max(case
      when questionSlug = 'are-you-worried-that-you-may-not-have-stable-housing-in-the-next-two-months' and answerSlug = 'yes' then 'Y'
      when questionSlug = 'are-you-worried-that-you-may-not-have-stable-housing-in-the-next-two-months' and answerSlug = 'no' then 'N'
      else null
    end) as isTheMemberHomelessOrAtRiskOfLosingShelter,
    
    max(case
      when questionSlug = 'lack-of-transportation-problem' and answerSlug = 'yes' then 'Y'
      when questionSlug = 'lack-of-transportation-problem' and answerSlug = 'no' then 'N'
      else null
    end) as doesMemberHaveDifficultyAccessingTransportation,
    
    max(case
      when questionSlug = 'explore-current-medical-diagnoses-with-member' and answerSlug != 'none' then 'Y'
      when questionSlug = 'explore-current-medical-diagnoses-with-member' and answerSlug = 'none' then 'N'
      else null
    end) as doesTheMemberHaveDiseaseBurdenOrTwoMoreComorbidities,
    
    max(case
      when questionSlug = 'is-there-anyone-you-can-really-count-on-when-you-need-help' and answerSlug = 'no' then 'Y'
      when questionSlug = 'is-there-anyone-you-can-really-count-on-when-you-need-help' and answerSlug = 'yes' then 'N'
      else null
    end) as doesTheMemberHaveLimitedSupportSystems,
    
    max(case
      when questionSlug = 'explore-with-the-member-their-current-mental-health-diagnoses' and answerSlug = 'depression' then 'Y'
      when questionSlug = 'explore-with-the-member-their-current-mental-health-diagnoses' and answerSlug != 'depression' then 'N'
      else null
    end) as doesTheMemberHaveActiveDiagnosisOfDepression,
    
    max(case
      when questionSlug = 'in-the-last-6-months-has-the-member-been-discharged-from-facility' and answerSlug = 'emergency-department' then 'Y'
      when questionSlug = 'in-the-last-6-months-has-the-member-been-discharged-from-facility' and answerSlug != 'emergency-department' then 'N'
      else null
    end) as doesTheMemberHaveOneOrMoreEdVisitsInTheLastSixMonths,
    
    max(case
      when questionSlug = 'in-the-last-6-months-has-the-member-been-discharged-from-facility' and 
      answerSlug in ('emergency-department','post-acute-care-facility', 'inpatient-or-observation-unit') then 'Y'
      when questionSlug = 'in-the-last-6-months-has-the-member-been-discharged-from-facility' and 
      answerSlug in ('no-recent-admission','unknown') then 'N'
      else null
    end) as hasTheMemberBeenHospitalizedInTheLastSixMonths,
    
    max(case
      when questionSlug = 'how-long-since-visited-doctor-for-checkup' and 
      answerSlug in ('within-the-past-2-years', 'within-the-past-5-years','more-than-5-years-ago') then 'Y'
      when questionSlug = 'how-long-since-visited-doctor-for-checkup' and 
      answerSlug in ('unknown', 'within-the-past-year','within-the-past-6-months') then 'N'
      else null
    end) as doesTheMemberHaveNoPcpRelationshipOrNoWellnessVisitsInOverOneYear,
   
    max(case
      when questionSlug = 'are-you-currently-receiving-substance-use-treatment' and answerSlug = 'yes' then 'Y'
      when questionSlug = 'are-you-currently-receiving-substance-use-treatment' and answerSlug = 'no' then 'N'
      else null
    end) as doesTheMemberHaveAHistoryOfSubstanceAbuse,
    
  from {{ ref('questions_answers_current') }}
  
  group by patientId
  
),

hard_to_reach_for_care_planning as (

  select
    patientId,
    case
      when isHardToReachCarePlanning then 'Y'
      else 'N'
    end as hardToReachStatusForCarePlanning

  from {{ ref('thpp_sla_hard_to_reach') }}

),

final as (
  select
    mi.memberId as MemberID,
    mi.firstName as Mbr_Fname,
    mi.lastName as Mbr_LName,
    mi.hardToReachOrEngageStatus as HTRStatus,
    mi.hardToReachOrEngageReason as HTR_Reason,
    mi.comprehensiveAssessmentCompleted as CompComplete,
    CAST(mi.comprehensiveAssessmentCompletedDate as STRING) as ComprComplDate,
    mi.mdsAssessmentCompleted as MDSComplete,
    CAST(mi.mdsAssessmentCompletedDate as STRING) as MDSComplDate,
    ct.currentLtsc as CurrLTSC,
    ct.isLtscPartOfIct as LTSC_ICT,
    la.evaluatedByLtscStatus as LTSC_Eval,
    coalesce(lo.ltssAssessmentbyLtscOffered, 'N') as LTSS_Offer,
    CAST(lo.ltssOfferDate AS STRING) as LTSS_OfferDate,
    lo.ltssAssessmentbyLtscAccepted as LTSS_Accept,
    lo.ltssAssessmentbyLtscNotAcceptedReason as LTSS_NoAccept_Reason,
    mi.carePlanRefusedFlag as CP_Refused,
    mi.carePlanDiscussedAndAgreedStatus as CP_Accepted,
    CAST(mi.carePlanDiscussedAndAgreedDate AS STRING) as CP_Accepted_Date,
    gi.latestGoalDescription as GoalDescr,
    gi.latestGoalStatus as GoalStatus,
    CAST(gi.latestGoalCreatedDate AS STRING) as GoalCreatedDate,
    CAST(gi.latestGoalUpdateDate AS STRING) as LastGoalUpdate,
    CAST(gi.latestGoalTargetDate AS STRING) as GoalTargetDate,
    CAST(gi.latestGoalDateMet AS STRING) as GoalMetDate,
    CAST(ldg.latestGoalDateUnmetAndDeleted AS STRING) as GoalUnMetDate,
    ldg.latestGoalUnmetReason as GoalUnMetReason,
    CAST(gi.latestInterventionUpdateDate AS STRING) as LastIntervUpdate,
    CAST(toc.transitionOfCareAssessmentCompletedDate AS STRING) as TOCAssessDate,
    ct.assignedCareManager as AssignCM,
    aq.DoYouConsiderYourCurrentHousingToBeAStableLivingArrangement as StableHousing,
    aq.ifNoDoYouConsiderYourselfHomeless as Homeless_Status,
    mi.doYouHaveAPcpYouGoToForYourHealthNeeds as PCPStatus,
    aq.areYouReceivingAnyPaidHomecareServicesPresently as HCS_Status,
    aq.massachusettsCommissionForTheBlind as MA_Blind_Status,
    null as PACT_Status,
    null as DDS_Status,
    null as ED_Use_Reason,
    null as ED_HC_Mgmt,
    null as DME_Home,
    aq.doYouSmokeCigarettes as Smoker,
    aq.doYouDrinkAlcoholicBeverages as Drinker,
    null as Illicit_Drug_Use,
    null as Interpreter_Svcs,
    aq.printedInADifferentLanguage as Multi_Lang_Print,
    null as SignLanguage,
    null as PCP_Assist,
    aq.doYouNeedSupportToAssistYouWithTransportationToMedicalAppointments as MedTransport_Status,
    aq.isTheMemberHomelessOrAtRiskOfLosingShelter as HousingLoss,
    aq.doesMemberHaveDifficultyAccessingTransportation as GenTransport_Status,
    aq.doesTheMemberHaveDiseaseBurdenOrTwoMoreComorbidities as Mult_Comorb_Status,
    aq.doesTheMemberHaveLimitedSupportSystems as Limit_Support_Status,
    null as Psych_Active_Status ,
    aq.doesTheMemberHaveActiveDiagnosisOfDepression as Depress_Active_Status,
    aq.doesTheMemberHaveOneOrMoreEdVisitsInTheLastSixMonths as ED_6Mos_Status,
    aq.hasTheMemberBeenHospitalizedInTheLastSixMonths as IPin6Mos,
    aq.doesTheMemberHaveNoPcpRelationshipOrNoWellnessVisitsInOverOneYear as Wellness_Status,
    aq.doesTheMemberHaveAHistoryOfSubstanceAbuse as SUD_History,
    null as Misc,
    mi.cityblockMemberState as CBH_Curr_State,
    CAST(mi.cityblockMemberStateStartDate AS STRING) as CBH_Curr_State_Date,
    CAST(mi.reportRunDate AS STRING) as CBH_AsOfDate,
    coalesce(htrcp.hardToReachStatusForCarePlanning, 'N') as HTRStatus_CarePlanning
    
    from member_info mi
    left join goal_intervention gi
    using (patientId)
    left join latest_deleted_goal_dates_with_comments ldg
    using (patientId)
    left join care_team ct
    using (patientId)
    left join ltss_assessment la 
    using (patientId)

    left join first_ltss_offers lo
      using (patientId)

    left join transition_of_care toc
    using (patientId)
    left join assessment_questions aq
    using (patientId)

    left join hard_to_reach_for_care_planning htrcp
    using (patientId)
    
)

select * from final
