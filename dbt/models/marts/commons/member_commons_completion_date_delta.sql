
with member_commons_completion_adj as (
select
  ms.patientId,
  mcc.minInitialAssessmentAt,
  mcc.acuityAssignedAt,
  mcc.minTaskAt,
  mcc.minAcpDocumentUploadAt,
  mcc.minCaseConferenceAt,
  mcc.minComprehensiveAssessmentAt,
  mcc.maxComprehensiveAssessmentAt,
  mcc.bothTuftsContractualAssessmentsAt
from {{ ref('member_states') }} as ms
left join {{ ref('member_commons_completion') }} as mcc
  using (patientId)),

member_commons_completion_date_delta as (

    select
        m.patientId,
        TIMESTAMP_DIFF(current_timestamp, ms.consentedAt, DAY) AS consentedToCurrentDays,
        TIMESTAMP_DIFF(minAcpDocumentUploadAt, ms.consentedAt, DAY) AS consentedToMinAcpDays,
        DATE_DIFF(DATE(current_timestamp), m.cohortGoLiveDate, DAY) AS cohortGoLiveToCurrentDays,
        DATE_DIFF(DATE(ms.consentedAt), m.cohortGoLiveDate, DAY) AS cohortGoLiveToConsentedDays,
        DATE_DIFF(DATE(ms.interestedAt), m.cohortGoLiveDate, DAY) AS cohortGoLiveToInterestedDays,
        DATE_DIFF(DATE(mcca.minInitialAssessmentAt), DATE(ms.consentedAt), DAY) AS consentedToInitialAssessmentDays,
        DATE_DIFF(DATE(mcca.acuityAssignedAt), DATE(ms.consentedAt), DAY) AS consentedToAcuityDays,
        DATE_DIFF(DATE(mcca.minTaskAt), DATE(ms.consentedAt), DAY) AS consentedToMapDays,
        CAST(DATE_DIFF(DATE(minCaseConferenceAt), DATE(ms.consentedAt), DAY) as NUMERIC) AS consentedToCaseConferenceDays,
        CAST(DATE_DIFF(DATE(consentedAt), DATE(interestedAt), DAY) as NUMERIC) AS interestedToConsentedDays,
        date_diff(date(bothTuftsContractualAssessmentsAt), date(consentedAt), day) as consentedToTuftsContractualAssessmentsDays,
        date_diff(date(minTaskAt), date(minComprehensiveAssessmentAt), day) as comprehensiveAssessmentToMapDays,
        date_diff(current_date(), date(maxComprehensiveAssessmentAt), DAY) as daysSinceLastComprehensiveAssessment,

    --need member table for cohortGoLiveDate
    from {{ ref('member') }} as m

    left join {{ ref('member_states') }} as ms
        on m.patientId = ms.patientId

    left join member_commons_completion_adj as mcca
        on m.patientId = mcca.patientId

)

select * from member_commons_completion_date_delta

