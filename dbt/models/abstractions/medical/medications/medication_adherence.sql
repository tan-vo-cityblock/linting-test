
with filtered_periods AS (

    SELECT DISTINCT
        map.patientId,
        mml.metric,
        map.drugClass,
        map.dateFilled,
        map.fillEnd

    from {{ ref('med_adherence_periods') }} as map

    inner join {{ source('hedis_codesets', 'mld_med_list_to_ndc') }} as mld
      on map.drugClass = mld.description

    inner join {{ source('hedis_codesets', 'cbh_metric_med_list') }} as mml
      on mld.medication_list = mml.medList

    left join {{ source('hedis_codesets', 'cbh_metric_excluded_class') }} as mec
      on mml.metric = mec.metric
        AND map.drugClass = mec.excludedClass

    WHERE mec.excludedClass IS NULL

),

partner_dates AS (

    SELECT
        p.partnerId,
        DATE_ADD(DATE_TRUNC(MAX(dateFilled), MONTH), INTERVAL 1 MONTH) AS exclusiveFinalPartnerDate

    FROM {{ ref('med_adherence_periods') }} as map

    INNER JOIN {{ source('commons', 'patient') }} as p
      ON map.patientId = p.id

    GROUP BY p.partnerId

),

member_months AS (

    select * from {{ ref('abs_member_months') }}

),

date_series as (

    SELECT `date`

    FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2017-01-01'), CURRENT_DATE(), INTERVAL 1 DAY)) AS `date`

    ORDER BY `date`

),

member_dates AS (

    SELECT DISTINCT
        lr.patientId,
        cs.metric,
        ds.date,
        fp.drugClass IS NOT NULL AS hasPrescription,
        COALESCE(ds.date = fp.dateFilled, FALSE) AS isPrescriptionDate,
        mm.eligibilityMonth IS NOT NULL AS isEligible,
        pd.exclusiveFinalPartnerDate,
        'claims' AS evidenceSource

    FROM {{ source('computed_fields', 'latest_results') }} as lr

    INNER JOIN {{ source('hedis_codesets', 'cbh_metric_condition_slug') }} as cs
      ON lr.fieldSlug = cs.conditionSlug
        AND lr.value != cs.negativeValue

    CROSS JOIN date_series as ds

    LEFT JOIN filtered_periods as fp
      ON lr.patientId = fp.patientId
        AND cs.metric = fp.metric
        AND ds.date BETWEEN fp.dateFilled AND fp.fillEnd

    LEFT JOIN member_months as mm
      ON lr.patientId = mm.patientId
        AND DATE_TRUNC(ds.date, MONTH) = mm.eligibilityMonth

    INNER JOIN {{ source('commons', 'patient') }} as p
      ON lr.patientId = p.id

    INNER JOIN partner_dates pd
      ON p.partnerId = pd.partnerId
        AND ds.date < pd.exclusiveFinalPartnerDate

)

select * from member_dates
