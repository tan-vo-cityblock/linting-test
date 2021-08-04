-- CTE 1: PULLING INFO
WITH cost_use_table as (

    SELECT
        patientId,
        inpatient,
        admissionId,
        date as dateFrom,
        dateTo,
        1 as admit
    FROM {{ ref('stg_encounters_claims') }}

),

--CTE2: Making sure all patients have claims_completion_month
claims_completion as (
 select
    *,
    -- Adding claims_completion_month date that is 3 months prior to accomdate for lag in receiving claims information
    DATE_SUB(DATE_TRUNC(DATE_SUB({{var("cm_reporting_claims_date_quarterly")}}, INTERVAL 3 MONTH), MONTH), INTERVAL 1 DAY) as claims_completion_month
 from {{ ref('cm_del_patients_quarterly') }} patients
),

-- CTE 3: APPLYING TRANSFORMATIONS
encounters AS (

    SELECT DISTINCT
        patientId,
        admissionId as Pat_clm_id,
        min(dateFrom) as adm_date
    FROM cost_use_table
    WHERE inpatient is true
    GROUP BY patientId, admissionId

),

encounters_with_completiondates as(
SELECT
claims_completion.*,
Pat_clm_id,
EXTRACT (YEAR from claims_completion_month)||'.'||EXTRACT (quarter from claims_completion_month) as claims_completion_quarter_year,
case when adm_date <= claims_completion_month then adm_date else null end as adm_date
FROM claims_completion
left join encounters using (patientid)
),

encounters_current_flags as (
select
patientId,
adm_date,
Pat_clm_id,
claims_completion_month,
claims_completion_quarter_year,
consentedAt,
coalesce(
       (
       (claims_completion_quarter_year >= EXTRACT (YEAR from consentedAt)||'.'||EXTRACT (QUARTER from consentedAt))
        AND (disenrolledAt is null OR claims_completion_quarter_year < EXTRACT (YEAR from disenrolledAt)||'.'||EXTRACT (QUARTER from disenrolledAt))
        ),false
        ) as case_active_current_claims_quarter
from encounters_with_completiondates ewc
),

encounters_flags_final as (
select
patientId,
adm_date,
Pat_clm_id,
claims_completion_month,
claims_completion_quarter_year,
case_active_current_claims_quarter,
coalesce(
    (case_active_current_claims_quarter = true
    AND claims_completion_quarter_year = EXTRACT (YEAR from adm_date)||'.'||EXTRACT (QUARTER from adm_date)
    AND date(consentedAt) <= adm_date)
    ,false) as hospitalized_current_claims_quarter
from encounters_current_flags
)

select * from encounters_flags_final
