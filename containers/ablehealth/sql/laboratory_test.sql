SELECT
  po.patient.patientId as patient_id,
  "LOINC" as code_type,
  results.Code as code_value,
  results.Value as result,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(po.order.CompletionDateTime.raw as TIMESTAMP)) as start_datetime
FROM
  `cityblock-data.medical.patient_orders` po
CROSS JOIN
  UNNEST(po.order.Results) AS results
WHERE po.patient.patientId IS NOT NULL
