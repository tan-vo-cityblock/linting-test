SELECT
  externalId AS patientId,
  codeType,
  codeValue,
  codeDescription,
  hccs,
  CASE
    WHEN status = "Not captured" THEN "OPEN"
    WHEN status = "Captured" THEN "CLOSED"
  ELSE
  "No coding gaps"
END
  AS opportunityStatus,
  createdAt
FROM
      {{ source('able', 'risk_suspects') }}