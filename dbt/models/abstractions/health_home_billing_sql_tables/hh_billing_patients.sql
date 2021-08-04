with hh_patients as (
SELECT
    pt.id as patientid,
    pt.insurance,
    pt.memberId as member_id,
    pt.medicaidID as CIN,
    pr.name as partner,
    category.name as category,
    ms.currentState,
    case when ms.attributedAt <= reporting_datetime then ms.attributedAt else null end as attributedAt,
    case when ms.consentedAt <= reporting_datetime then ms.consentedAt else null end as consentedAt,
    case when ms.enrolledAt <= reporting_datetime then ms.enrolledAt else null end as enrolledAt,
    case when ms.disenrolledAt <= reporting_datetime then ms.disenrolledAt else null end as disenrolledAt,
    mem.cohortGoLiveDate as cohortAttributionDate,
    baseline.* except (patientid),
    interventions.* except (patientid),
    care_plan.* except (patientId),
    rd.*,
    other_assessments.* except (patientid)
FROM {{ source('commons', 'patient') }} pt
LEFT JOIN {{ source('member_index', 'member') }} pic
ON pt.id = pic.id
LEFT JOIN {{ source('member_index', 'partner') }} pr
ON pr.id = pic.partnerId
LEFT JOIN  {{ ref('member_states') }} ms
ON pt.id = ms.patientId
LEFT JOIN {{ source('member_index', 'category') }} category
ON pic.categoryId = category.id
LEFT JOIN {{ ref('member') }} mem
ON pt.id = mem.patientId
LEFT JOIN {{ source('commons', 'patient_disenrollment') }} pd
ON pt.id = pd.patientId AND pd.deletedAt is null
LEFT JOIN {{ ref('hh_billing_insurance') }} insurance
    ON pt.id = insurance.patientId
LEFT JOIN {{ ref('hh_billing_baseline_assessments') }} baseline
    ON pt.id = baseline.patientId
LEFT JOIN {{ ref('hh_billing_interventions') }} interventions
    ON pt.id = interventions.patientId
LEFT JOIN {{ ref('hh_billing_generated_care_plan') }} care_plan
    ON pt.id = care_plan.patientId
inner join {{ ref('hh_billing_reporting_dates') }} rd
    on pic.createdat <= rd.reporting_datetime
LEFT JOIN {{ source('commons', 'patient_document') }} pdoc
    on pt.id = pdoc.patientid
LEFT JOIN {{ ref('hh_billing_other_assessments') }} other_assessments
    ON pt.id = other_assessments.patientId
WHERE pic.cohortId > 0
and pr.name = 'emblem'
and pt.cohortName <> 'Emblem Medicaid Digital Cohort 1'
--only take patients who've consented to health home services
and pdoc.documentType in ('healthHomeInformationConsent','verbalHealthHomeInformationConsent')
and pdoc.deletedAt is null
)

select * from hh_patients
where (attributedAt is not null
    and consentedAt is not null
    and disenrolledAt is null)
