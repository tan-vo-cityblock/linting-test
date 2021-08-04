with member_month as (
    select 
      patientId, 
      format_date('%Y-%m', month) as month
  from {{ ref('cm_consented_member_info') }},
  unnest(generate_date_array(date((select date_trunc(date(consentedAt), month))), date_trunc(current_date(), month), interval 1 month)) as month ),

baseline as (
  select 
    patientId,
    minEssentialsStartedAt as baselineAssessmentInitiatedAt,
    minEssentialsCompletedAt as baselineAssessmentCompletedAt
  from {{ ref('essential_tools') }}  ),

map as (
  select 
    patientId,
    minTaskAt as mapInitiatedAt,
    minCaseConferenceAt as intialCaseConferenceAt,
    daysSinceLastCaseConference
  from {{ ref('member_commons_completion')}}  ),

delta as (
  select 
    patientId,
    consentedToCurrentDays,
    case 
      when consentedToMapDays >= 0 then consentedToMapDays
      when consentedToMapDays < 0 then 0
    end as consentedToMapDays
  from {{ ref('member_commons_completion_date_delta') }}  ),
  
impressions as (
  select 
      patientId,
      summary
  from {{ source('commons','patient_health_summary') }}
  where deletedAt is null ),

tends as (
  select 
    patientId,
    format_timestamp('%Y-%m', timestamp(format_timestamp('%F %T', eventTimestamp , 'America/New_York'))) AS month,
    count(distinct case when isSuccessfulTend then memberInteractionKey else null end) AS tendsPerMonth
  from {{ ref('member_interactions') }}
  group by patientId, month),

final as (
  select distinct 
    mm.patientId,
    mi.fullName,
    mi.dateOfBirth,
    coalesce(mi.primaryCommunityHealthPartnerName, 'N/A') as primaryCommunityHealthPartnerName,
    mi.currentState,
    mi.consentedAt,
    d.consentedToCurrentDays,
    ma.mapInitiatedAt, 
    d.consentedToMapDays,
    b.baselineAssessmentInitiatedAt,
    b.baselineAssessmentCompletedAt,
    case
      when date_diff(date(b.baselineAssessmentCompletedAt), date(mi.consentedAt), day) >= 0 then date_diff(date(b.baselineAssessmentCompletedAt), date(mi.consentedAt), day)
      when date_diff(date(b.baselineAssessmentCompletedAt), date(mi.consentedAt), day) < 0 then 0
    end as consentedToBaselineAssessmentDays,
    case 
      when b.baselineAssessmentCompletedAt is not null and date_diff(date(b.baselineAssessmentCompletedAt), date(mi.consentedAt), day) <= 60 then 'Yes'
      when b.baselineAssessmentCompletedAt is not null and date_diff(date(b.baselineAssessmentCompletedAt), date(mi.consentedAt), day) > 60 then 'No'
      when b.baselineAssessmentCompletedAt is null and d.consentedToCurrentDays > 60 then 'No'
      else 'N/A' 
    end as baselineCompletedWithin60Days,
    case 
      when b.baselineAssessmentCompletedAt is not null and p.summary is not null then 'Yes'
      when b.baselineAssessmentCompletedAt is not null and p.summary is null then 'No'
      else 'N/A'
    end as memberImpressionsCompleted,
    case
      when ma.mapInitiatedAt is not null and d.consentedToMapDays <= 30 then 'Yes'
      when ma.mapInitiatedAt is not null and d.consentedToMapDays > 30 then 'No' 
      else 'N/A'
    end as mapIntiatedWithin30DaysOfConsent,
    mi.totalGoalsCompleted, 
    ma.intialCaseConferenceAt,
    ma.daysSinceLastCaseConference,
    case 
      when consentedToCurrentDays >= 90 and daysSinceLastCaseConference <= 92 then 'Yes'
      when consentedToCurrentDays >= 90 and daysSinceLastCaseConference > 92 then 'No'
      else 'N/A'
    end as quarterlyCaseConferenceConducted,
    coalesce(mi.initialMemberAcuityDescription,'N/A') as initialMemberAcuityDescription,
    coalesce(mi.currentMemberAcuityDescription,'N/A') as currentMemberAcuityDescription,
    mm.month,
    case
      when t.tendsPerMonth is not null then t.tendsPerMonth
      when t.tendsPerMonth is null then 0
    end as tendsPerMonth,
    case 
      when mi.currentMemberAcuityDescription is not null and t.tendsPerMonth >= mi.targetMonthlyTendsCurrentAcuity then 'Yes'
      when mi.currentMemberAcuityDescription is not null and t.tendsPerMonth < mi.targetMonthlyTendsCurrentAcuity then 'No'
      else 'N/A'
    end as minimumTendsMetForCurrentAcuity, 
    case
      when mi.currentMemberAcuityDescription = 'Stable' and t.tendsPerMonth = 0 then 'Low - Inappropriate'
      when mi.currentMemberAcuityDescription = 'Stable' and t.tendsPerMonth between 1 and 2 then 'Appropriate' 
      when mi.currentMemberAcuityDescription = "Stable" and t.tendsPerMonth >= 3 then 'High - Consider Status Change'
      when mi.currentMemberAcuityDescription = 'Mild' and t.tendsPerMonth between 0 and 1 then 'Low - Inappropriate'
      when mi.currentMemberAcuityDescription = 'Mild' and t.tendsPerMonth between 2 and 3 then 'Appropriate' 
      when mi.currentMemberAcuityDescription = "Mild" and t.tendsPerMonth >= 4 then 'High - Consider Status Change'
      when mi.currentMemberAcuityDescription = 'Moderate' and t.tendsPerMonth between 0 and 3 then 'Low - Inappropriate'
      when mi.currentMemberAcuityDescription = 'Moderate' and t.tendsPerMonth between 4 and 5 then 'Appropriate' 
      when mi.currentMemberAcuityDescription = "Moderate" and t.tendsPerMonth >= 6 then 'High - Consider Status Change'
      when mi.currentMemberAcuityDescription = 'Severe' and t.tendsPerMonth between 0 and 5 then 'Low - Inappropriate'
      when mi.currentMemberAcuityDescription = 'Severe' and t.tendsPerMonth between 6 and 7 then 'Appropriate' 
      when mi.currentMemberAcuityDescription = "Severe" and t.tendsPerMonth >= 8 then 'High - Consider Status Change'
      when mi.currentMemberAcuityDescription = 'Critical' and t.tendsPerMonth between 0 and 5 then 'Low - Inappropriate'
      when mi.currentMemberAcuityDescription = 'Critical' and t.tendsPerMonth >= 6 then 'Appropriate'
      else 'N/A'
    end as appropriateTendsForCurrentAcuity
  from member_month mm
  left join {{ ref('cm_consented_member_info') }} mi
  using (patientId)
  left join baseline b
  using (patientId)
  left join map ma
  using (patientId)
  left join delta d
  using (patientId)
  left join impressions p
  using (patientId)
  left join tends t
  on mm.patientId = t.patientId and mm.month = t.month)

select * from final