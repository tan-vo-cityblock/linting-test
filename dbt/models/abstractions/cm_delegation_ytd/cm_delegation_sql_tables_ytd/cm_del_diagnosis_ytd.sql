-- Identify top diagnoses per member
--BQ: https://console.cloud.google.com/bigquery?sq=915813449633:e16c1744f9844b4d92b82030b836dab0


WITH all_diag_codes AS (

  SELECT DISTINCT
    memberIdentifier as patientId,
    diagnosisCode,
    diagnosisCodeset as diagnosisCodeType,
    sourceId as claimId,
    serviceDateFrom as fromDate
  FROM {{ ref('abs_diagnoses') }}
--we want to make sure fromDate is between delegation date AND reporting date.
--Other dates act the same but if they are after the reporting date, they are null
  inner join {{ ref('cm_del_reporting_dates_ytd') }}
    on serviceDateFrom <= reporting_date
  INNER join {{ ref('cm_del_delegation_dates_ytd') }} dd
     on date(serviceDateFrom) >= delegation_at
     and memberIdentifier = dd.patientid
  LEFT JOIN unnest(claimLineStatuses) as claimLineStatus

  WHERE
    diagnosisCodeset = 'ICD10Cm' and
    claimLineStatus in ('Paid', 'Encounter') and
    sourceType = 'claim' and
    memberIdentifierField = 'patientId'

),

--https://github.com/cityblock/mixer/blob/e593c3a2049b3e945ca84b0e7c978fd4246dadf8/containers/cm_delegation_reports/step2_transform.R#L250
countclaims as (
select
  patientId,
  diagnosisCode,
  count(distinct (claimId)) as count_claims,
--This is used instead of rank because this will break the ties AND rank the top diagnosis's by count_claims
  ROW_NUMBER()OVER(partition by patientid
    order by count(distinct (claimId)) desc) as ranking
from all_diag_codes
group by 1,2
)

select
    patientid,
    max(case when ranking = 1 then diagnosisCode else null end) as primary_diagnosis,
    max(case when ranking = 2  then diagnosisCode else null end) as secondary_diagnosis,
    current_date as date_run
from countclaims
where ranking <= 2
group by 1
