--this query has all the information from below: https://console.cloud.google.com/bigquery?sq=915813449633:66a71be5807b4ce79dfc2267ddbcdf2b

--we want exclude members who consented (or disenrolled/enrolled) prior to delegation dates
--if a member disenroll and then re-consent, we want to take their minimum consent date AFTER delegation date
with patients as (
SELECT
    m.patientId,
    upper(pt.firstName) AS firstName,
    upper(pt.lastName) AS lastName,
    pt.middleName,
    pt.dateOfBirth,
    DATE_DIFF(CURRENT_DATE(), CAST(pt.dateOfBirth AS DATE), YEAR) as age,
    pt.insurance,
    pt.memberId,
    pt.medicaidID as CIN,
    pt.productDescription,
    CASE
        when pt.productDescription like '%dual%' or lower(pt.productDescription) like '%dsnp%' then 'DSNP'
        when pt.lineOfBusiness = 'Commercial' and insurance.sublineOfBusiness = 'Exchange'
            or pt.lineOfBusiness = 'Commercial' and pt.productDescription = 'Commercial - Exchange'
        then 'COMMERCIAL EXCHANGE'
        when pt.lineOfBusiness in ('hmo', 'ps') then 'COMMERCIAL'
        when pt.lineOfBusiness ='M' then "MEDICARE"
        else upper(pt.lineOfBusiness) end as line_of_business,
    pt.medicaidPremiumGroup,
    pt.pcpName,
    pt.pcpPractice,
    pr.name as partner,
    pic.cohortId as cohort,
    pt.cohortName,
    cat.name as category,
    ms.currentState,
    case when extract (year from attributedAt)||'.'||extract (quarter from attributedAt)  = reporting_quarter_yr then ms.attributedAt else null end as attributedAt_tp,
    case when  extract (year from assignedAt)||'.'||extract (quarter from assignedAt)  =  reporting_quarter_yr then ms.assignedAt else null end as assignedAt_tp,
    case when extract (year from contactAttemptedAt)||'.'||extract (quarter from contactAttemptedAt)  = reporting_quarter_yr then ms.contactAttemptedAt else null end as contactAttemptedAt_tp,
    case when extract (year from reachedAt)||'.'||extract (quarter from reachedAt)  = reporting_quarter_yr then ms.reachedAt else null end as reachedAt_tp,
    case when extract (year from interestedAt)||'.'||extract (quarter from interestedAt)  = reporting_quarter_yr then ms.interestedAt else null end as interestedAt_tp,
    case when extract (year from veryInterestedAt)||'.'||extract (quarter from veryInterestedAt)  = reporting_quarter_yr then ms.veryInterestedAt else null end as veryInterestedAt_tp,
    case when extract (year from notInterestedAt)||'.'||extract (quarter from notInterestedAt)  = reporting_quarter_yr then ms.notInterestedAt else null end as notInterestedAt_tp,
    case when  extract (year from consentedAt)||'.'||extract (quarter from consentedAt) = reporting_quarter_yr then ms.consentedAt else null end as consentedAt_tp,
    case when extract (year from enrolledAt)||'.'||extract (quarter from enrolledAt) = reporting_quarter_yr then ms.enrolledAt else null end as enrolledAt_tp,
    case when extract (year from disenrolledAt)||'.'||extract (quarter from disenrolledAt)  = reporting_quarter_yr then ms.disenrolledAt else null end as disenrolledAt_tp,
-----this is for cm_del_encounters and cm_delegation_summary report hospitalizations since there is a 3 month lag in claims----------
    case when ms.attributedAt <= reporting_datetime then ms.attributedAt else null end as attributedAt,
    case when ms.assignedAt <= reporting_datetime then ms.assignedAt else null end as assignedAt,
    case when ms.contactAttemptedAt <= reporting_datetime then ms.contactAttemptedAt else null end as contactAttemptedAt,
    case when ms.reachedAt <= reporting_datetime then ms.reachedAt else null end as reachedAt,
    case when ms.interestedAt <= reporting_datetime then ms.interestedAt else null end as interestedAt,
    case when ms.veryInterestedAt <= reporting_datetime then ms.veryInterestedAt else null end as veryInterestedAt,
    case when ms.notInterestedAt <= reporting_datetime then ms.notInterestedAt else null end as notInterestedAt,
    case when ms.consentedAt <= reporting_datetime then ms.consentedAt else null end as consentedAt,
    case when ms.enrolledAt <= reporting_datetime then ms.enrolledAt else null end as enrolledAt,
    case when ms.disenrolledAt <= reporting_datetime then ms.disenrolledAt else null end as disenrolledAt,
    ms.disenrollmentReason,
    --helps with hard to reach logic
    case when date(attributedAt) < cohortGoLiveDate then cohortGoLiveDate
        else date(attributedAt) end as cohort_or_attributed_date,
    m.cohortGoLiveDate AS cohortAttributionDate,
    pt.doNotCall,
    dd.delegation_at,
    Rd.*,
    case when minInitialAssessmentAt <= reporting_datetime then minInitialAssessmentAt else null end as minInitialAssessmentAt,
    case when extract (year from minInitialAssessmentAt)||'.'||extract (quarter from minInitialAssessmentAt)  = reporting_quarter_yr
        then minInitialAssessmentAt else null end as minInitialAssessmentAt_tp,
FROM {{ source('commons', 'patient') }} pt, {{ ref('cm_del_reporting_dates_quarterly') }} rd
LEFT JOIN {{ source('member_index', 'member') }} pic
    ON pt.id = pic.id
LEFT JOIN {{ ref('member') }} m
    ON pt.id = m.patientId
LEFT JOIN  {{ ref('member_states') }} ms
    ON pt.id = ms.patientId
LEFT JOIN  {{ source('commons', 'partner') }} pr
    ON pt.partnerId = pr.id
LEFT JOIN {{ source('member_index', 'category') }} cat
    ON pic.categoryId = cat.id
INNER join {{ ref('cm_del_delegation_dates_ytd') }} dd
    on pt.id = dd.patientid
    and pr.name = dd.partner_list
LEFT JOIN {{ ref('cm_del_insurance_quarterly') }} insurance
    ON pt.id = insurance.patientId
left join {{ ref('member_commons_completion') }} mcc
    on pt.id = mcc.patientid

WHERE lower(m.cohortName) not like '%digital%'
and (date(ms.consentedAt) >= date(dd.delegation_at)
OR (date(ms.disenrolledAt) >= date(dd.delegation_at) and date(ms.consentedAt) is null)
OR (consentedAt is null AND disenrolledAt is null))
and pt.insurance not in ('carefirst')
)

select * from patients
