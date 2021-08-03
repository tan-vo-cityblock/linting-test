
{% macro medication_fill(slug,
                         hedis_medication_list,
                         condition_slugs,
                         min_female_age=None,
                         min_male_age=None,
                         max_age=None,
                         excluded_condition_slugs=None,
                         excluded_hedis_medication_list=None,
                         pharma_source_names=["claims"],
                         washout_period="2 MONTH",
                         lookback_period="210 DAY",
                         treatment_period="120 DAY",
                         severe_min_pdc=".4",
                         severe_max_pdc=".5",
                         moderate_max_pdc=".7",
                         mild_max_pdc=".9",
                         moderate_max_abs="7",
                         moderate_min_abs="5",
                         mild_min_abs="3",
                         final_abs_period_days="8",
                         age_table=ref('member_info'),
                         table=ref('med_adherence_periods')
                       )%}

{% set medication_stat_abs_filled="SUM(CAST(hasPrescription AS INT64) * CAST(isEligible AS INT64))" %}
{% set medication_stat_abs_missed="SUM(CAST(isEligible AS INT64)) - " ~ medication_stat_abs_filled %}
{% set medication_stat_pdc="SAFE_DIVIDE(" ~ medication_stat_abs_filled ~ ", SUM(CAST(isEligible AS INT64)))" %}

{% set hedis_medication_list_string=list_to_sql(hedis_medication_list) %}
{% set pharma_source_string=list_to_sql(pharma_source_names) %}

{% if excluded_hedis_medication_list %}
  {% set excluded_hedis_medication_list_string=list_to_sql(excluded_hedis_medication_list) %}
{% endif %}

{{ find_eligible_members(age_table, min_female_age, min_male_age, max_age, condition_slugs, table, excluded_condition_slugs) }}

filtered_periods as (

    select distinct
        ma.patientId,
        ma.drugClass,
        ma.dateFilled,
        ma.fillEnd

    from {{ table }} as ma

    inner join {{ source('hedis_codesets', 'mld_med_list_to_ndc') }} as mld
      on ma.drugClass = mld.description
        and mld.medication_list in {{ hedis_medication_list_string }}

    {% if excluded_hedis_medication_list %}
    where ma.drugClass not in {{ excluded_hedis_medication_list_string }}
    {% endif %}

),

partner_dates as (

    select
        p.partnerId,
        DATE_ADD(DATE_TRUNC(MAX(dateFilled), MONTH), INTERVAL 1 MONTH) AS exclusiveFinalPartnerDate

    from {{ table }} as ma

    inner join {{ source('commons', 'patient') }} as p
      on ma.patientId = p.id

    group by p.partnerId

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
      em.patientId,
      ds.date,
      fp.drugClass IS NOT NULL AS hasPrescription,
      mm.eligibilityMonth IS NOT NULL AS isEligible,
      pd.exclusiveFinalPartnerDate

    FROM eligible_members em

    CROSS JOIN date_series as ds

    LEFT JOIN filtered_periods as fp
      ON em.patientId = fp.patientId
        AND ds.date BETWEEN fp.dateFilled AND fp.fillEnd

    LEFT JOIN member_months as mm
      ON em.patientId = mm.patientId
        AND DATE_TRUNC(ds.date, MONTH) = mm.eligibilityMonth

    INNER JOIN {{ source('commons', 'patient') }} as p
      ON em.patientId = p.id

    INNER JOIN partner_dates as pd
      ON p.partnerId = pd.partnerId
        AND ds.date < pd.exclusiveFinalPartnerDate

),

washout as (

    select
        * EXCEPT(exclusiveFinalPartnerDate),
        DATE_SUB(exclusiveFinalPartnerDate, INTERVAL {{ washout_period }}) AS exclusiveFinalPartnerWashoutDate,
        'claims' AS evidenceSource

    from member_dates

),

lookback as (

    select
        patientId,
        `date`,
        hasPrescription,
        isEligible,
        exclusiveFinalPartnerWashoutDate

    from washout

    where `date` >= DATE_SUB(exclusiveFinalPartnerWashoutDate, INTERVAL {{ lookback_period }})
      and `date` < exclusiveFinalPartnerWashoutDate
      and evidenceSource in {{ pharma_source_string }}

),

min_dates as (

    select
        patientId,
        MIN(`date`) AS minDate

    from lookback

    where hasPrescription

    group by patientId

),

treatment as (

    select
        l.patientId,
        l.date,
        l.hasPrescription,
        l.isEligible,
        ROW_NUMBER() OVER(PARTITION BY l.patientId ORDER BY l.date DESC) AS revDayNum

    from lookback as l

    join  min_dates as md
      using (patientId)

    WHERE l.date >= DATE_SUB(l.exclusiveFinalPartnerWashoutDate, INTERVAL {{ treatment_period }})
      AND l.date < l.exclusiveFinalPartnerWashoutDate
      AND l.date >= md.minDate

),

pdc as (

    SELECT
        patientId,
        -- Convert boolean values to ints: 0 for false, 1 for true
        SUM(CAST(isEligible AS INT64)) AS measurementLength,
        {{ medication_stat_abs_missed }} AS measurementMissingCount,

        CASE
          WHEN ({{ medication_stat_pdc }}  <  {{ severe_min_pdc }}) THEN 4
          WHEN ({{ medication_stat_pdc }} <= {{ severe_max_pdc }}) THEN 3
          WHEN ({{ medication_stat_pdc }} <= {{ moderate_max_pdc }}) THEN 2
          WHEN ({{ medication_stat_pdc }} <= {{ mild_max_pdc }}) THEN 1
          WHEN ({{ medication_stat_pdc }} >  {{ mild_max_pdc }}) THEN 0
          END
        AS valuePDC

    FROM treatment

    GROUP BY patientId

),

abs AS (

    SELECT
        patientId,
        -- Convert boolean values to ints: 0 for false, 1 for true
        {{ medication_stat_abs_missed }} AS finalMissingCount,

        CASE
          WHEN ({{ medication_stat_abs_missed }} >  {{ moderate_max_abs }}) THEN 3
          WHEN ({{ medication_stat_abs_missed }} >= {{ moderate_min_abs }}) THEN 2
          WHEN ({{ medication_stat_abs_missed }} >= {{ mild_min_abs }}) THEN 1
          WHEN ({{ medication_stat_abs_missed }} <  {{ mild_min_abs }}) THEN 0
          END
        AS valueAbs

    FROM treatment

    WHERE revDayNum IN (SELECT * FROM UNNEST(GENERATE_ARRAY(1, {{ final_abs_period_days }})))

    GROUP BY patientId

),

joint AS (

    SELECT
        pdc.patientId,
        CASE
          WHEN pdc.valuePDC > abs.valueAbs THEN pdc.valuePDC
          ELSE abs.valueAbs
          END
        AS maxValuePDCAbs

    FROM pdc

    JOIN abs
      USING (patientId)

    WHERE measurementLength > 0

),

cf_status AS (

    SELECT
        patientId,

        CASE
          WHEN maxValuePDCAbs = 0 THEN 'stable'
          WHEN maxValuePDCAbs = 1 THEN 'mild'
          WHEN maxValuePDCAbs = 2 THEN 'moderate'
          WHEN maxValuePDCAbs = 3 THEN 'severe'
          WHEN maxValuePDCAbs = 4 THEN 'critical'
          END
        AS value

    FROM joint

),

final as (

    {{ computed_field(slug=slug, type='medication_fill', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
