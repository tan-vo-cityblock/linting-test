SELECT
  patientId,
  consentedToMapDays
FROM 
  {{ ref('member_commons_completion_date_delta') }} 