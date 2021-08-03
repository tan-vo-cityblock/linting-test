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
    DATE_SUB(DATE_TRUNC(DATE_SUB({{var("cm_reporting_claims_date_monthly")}}, INTERVAL 3 MONTH), MONTH), INTERVAL 1 DAY) as claims_completion_month
 from {{ ref('cm_del_patients_monthly') }} patients
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
EXTRACT (YEAR from claims_completion_month)||'.'||lpad(cast(extract(month from date(claims_completion_month)) as string), 2, '0') as claims_completion_month_year,
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
claims_completion_month_year,
consentedAt,
coalesce(
        (
        (claims_completion_month_year >= EXTRACT (YEAR from consentedAt)||'.'||lpad(cast(extract(month from date(consentedAt)) as string), 2, '0'))
         AND (disenrolledAt is null OR claims_completion_month_year < EXTRACT (YEAR from disenrolledAt)||'.'||lpad(cast(extract(month from date(disenrolledAt)) as string), 2, '0'))
         ),false
        ) as case_active_current_claims_month,
from encounters_with_completiondates ewc
),

encounters_flags_final as (
select
patientId,
adm_date,
Pat_clm_id,
claims_completion_month,
claims_completion_month_year,
case_active_current_claims_month,
coalesce(
    (case_active_current_claims_month = true
    AND claims_completion_month_year = EXTRACT (YEAR from adm_date)||'.'||lpad(cast(extract(month from date(adm_date)) as string), 2, '0')
    AND date(consentedAt) <= adm_date)
    ,false) as hospitalized_current_claims_month,
from encounters_current_flags
)

select * from encounters_flags_final
