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
    case when ms.attributedAt between  rd.reporting_datetime_first and rd.reporting_datetime_last then ms.attributedAt else null end as attributedAt_tp,
    case when ms.assignedAt between  rd.reporting_datetime_first and rd.reporting_datetime_last then ms.assignedAt else null end as assignedAt_tp,
    case when ms.contactAttemptedAt between  rd.reporting_datetime_first and rd.reporting_datetime_last then ms.contactAttemptedAt else null end as contactAttemptedAt_tp,
    case when ms.reachedAt between  rd.reporting_datetime_first and rd.reporting_datetime_last then ms.reachedAt else null end as reachedAt_tp,
    case when ms.notInterestedAt between  rd.reporting_datetime_first and rd.reporting_datetime_last then ms.notInterestedAt else null end as notInterestedAt_tp,
    case when ms.veryInterestedAt between  rd.reporting_datetime_first and rd.reporting_datetime_last then ms.veryInterestedAt else null end as veryInterestedAt_tp,
    case when ms.interestedAt between  rd.reporting_datetime_first and rd.reporting_datetime_last then ms.interestedAt else null end as interestedAt_tp,
    case when ms.consentedAt between  rd.reporting_datetime_first and rd.reporting_datetime_last then ms.consentedAt else null end as consentedAt_tp,
    case when ms.enrolledAt between  rd.reporting_datetime_first and rd.reporting_datetime_last then ms.enrolledAt else null end as enrolledAt_tp,
    case when ms.disenrolledAt between  rd.reporting_datetime_first and rd.reporting_datetime_last then ms.disenrolledAt else null end as disenrolledAt_tp,
-----this is for cm_del_encounters and cm_delegation_summary report hospitalizations since there is a 3 month lag in claims----------
    case when ms.attributedAt <= reporting_datetime_last then ms.attributedAt else null end as attributedAt,
    case when ms.assignedAt <= reporting_datetime_last then ms.assignedAt else null end as assignedAt,
    case when ms.contactAttemptedAt <= reporting_datetime_last then ms.contactAttemptedAt else null end as contactAttemptedAt,
    case when ms.reachedAt <= reporting_datetime_last then ms.reachedAt else null end as reachedAt,
    case when ms.interestedAt <= reporting_datetime_last then ms.interestedAt else null end as interestedAt,
    case when ms.veryInterestedAt <= reporting_datetime_last then ms.veryInterestedAt else null end as veryInterestedAt,
    case when ms.notInterestedAt <= reporting_datetime_last then ms.notInterestedAt else null end as notInterestedAt,
    case when ms.consentedAt <= reporting_datetime_last then ms.consentedAt else null end as consentedAt,
    case when ms.enrolledAt <= reporting_datetime_last then ms.enrolledAt else null end as enrolledAt,
    case when ms.disenrolledAt <= reporting_datetime_last then ms.disenrolledAt else null end as disenrolledAt,
    ms.disenrollmentReason,
 --helps with hard to reach logic
    case when date(attributedAt) < cohortGoLiveDate then cohortGoLiveDate
        else date(attributedAt) end as cohort_or_attributed_date,
    m.cohortGoLiveDate AS cohortAttributionDate,
    pt.doNotCall,
    fp.* except (patientId),
    diagnosis.primary_diagnosis,
    diagnosis.secondary_diagnosis,
    dd.delegation_at,
    rd.*,
    case when minInitialAssessmentAt <= reporting_datetime_last then minInitialAssessmentAt else null end as minInitialAssessmentAt,
    case when minInitialAssessmentAt between reporting_datetime_first and reporting_datetime_last
        then minInitialAssessmentAt else null end as minInitialAssessmentAt_tp,
FROM {{ source('commons', 'patient') }} pt, {{ ref('cm_del_reporting_dates_monthly') }} rd
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
LEFT JOIN {{ ref('cm_del_procedures_monthly') }}  fp
    ON pt.id = fp.patientId
LEFT JOIN {{ ref('cm_del_diagnosis_monthly') }} diagnosis
    ON pt.id = diagnosis.patientId
LEFT JOIN {{ ref('cm_del_insurance_monthly') }} insurance
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
