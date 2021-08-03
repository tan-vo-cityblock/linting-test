WITH
  baseline_completion AS (
  SELECT
    patientId,
    CASE
      WHEN consentedToInitialAssessmentDays <= 45 THEN 'Yes'
      WHEN consentedToInitialAssessmentDays > 45 THEN 'No'
    ELSE 'Incomplete'
  END
    AS baselineWithin45Days
  FROM {{ ref('member_commons_completion_date_delta') }} ),
  
  member_impressions AS (
  SELECT
    patientId,
    summary
  FROM
    {{ source('commons','patient_health_summary') }}
  WHERE
    deletedAt IS NULL)

SELECT
  bc.patientID AS patientId,
  bc.baselineWithin45Days AS baselineWithin45Days,
  mi.summary AS memberImpressions
FROM
  baseline_completion bc
INNER JOIN
  member_impressions mi
ON
  bc.patientId = mi.patientId