WITH

latest_address as (
  SELECT * from
  (
    SELECT
      address.*,
      RANK() OVER (PARTITION BY address.memberId ORDER BY address.spanDateStart DESC, address.SpanDateEnd DESC ) AS rnk
    FROM
      `cbh-db-mirror-prod.member_index_mirror.address` address
    WHERE
      deletedAt is null) t
   where rnk = 1
),

latest_phone as (
  SELECT * from
  (
    SELECT 
      phone.*,
      RANK() OVER (PARTITION BY phone.memberId ORDER BY phone.updatedAt DESC, phone.createdAt DESC ) AS rnk
    FROM
      `cbh-db-mirror-prod.member_index_mirror.phone` phone
    WHERE
      deletedAt is null) t
   where rnk = 1
),

withdrawn_healthix AS (
  SELECT
    patientId,
    createdAt
    FROM
      `cbh-db-mirror-prod.commons_mirror.patient_document`
    WHERE
      documentType = "hieHealthixWithdrawal"
      AND deletedAt IS NULL
  ),
  
consented_patients AS (
  SELECT
    t.patientId,
    t.createdAt
  FROM (
    SELECT
      t.*,
      RANK() OVER (PARTITION BY t.patientId ORDER BY t.createdAt DESC ) AS rnk
    FROM
      `cbh-db-mirror-prod.commons_mirror.patient_document` t
    LEFT JOIN
      withdrawn_healthix
    ON
      withdrawn_healthix.patientId = t.patientId
      AND withdrawn_healthix.createdAt > t.createdAt
    WHERE
      t.documentType = "hieHealthixConsent"
      AND withdrawn_healthix.patientId IS NULL ) t
    WHERE
      rnk = 1
)

SELECT
  "CITYBLOCK" AS AssigningAuthority,
  "1.7" AS Version,
  m.id AS MRN,
  md.lastName as LastName,
  md.firstName AS FirstName,
  md.middleName AS MiddleName,
  "" AS NA,
  FORMAT_DATE("%Y%m%d", CAST(md.dateOfBirth as DATE)) AS DateOfBirth,
  SAFE.SUBSTR(upper(md.sex), 0, 1) AS Gender,
  address.street1 AS AddressLine1,
  address.street2 AS AddressLine2,
  address.city AS City,
  address.state AS State,
  address.zip AS Zip,
  "USA" AS Country,
  address.county AS County,
  phone.phone AS PhoneNumber,
  "" AS NA2,
  CONCAT(SUBSTR(md.ssn, 1, 3),'-', SUBSTR(md.ssn, 4, 2),'-', SUBSTR(md.ssn, 3, 4)) AS SSN,
  CASE
    WHEN consented_patients.patientId IS NOT NULL THEN 'Y'
    ELSE NULL
  END AS Consent,
  FORMAT_TIMESTAMP("%Y%m%d%H%M%S",CURRENT_TIMESTAMP()) AS LoadDate,
  "" AS MedicaidID, -- TODO: See if we can get this
  "" AS TypeOfVisit,
  "" AS VisitBeginDate,
  "" AS VisitEndDate,
  "" AS ProviderLastName,
  "" AS ProviderFirstName,
  CASE
    WHEN consented_patients.patientId IS NOT NULL THEN FORMAT_TIMESTAMP("%Y%m%d",consented_patients.createdAt)
    ELSE ''
  END AS ConsentEffectiveDate,
  "" AS EncounterID,
  "" AS Allergies,
  "" AS Procedure,
  "" AS Diagnosis,
FROM
  `cbh-db-mirror-prod.member_index_mirror.member` m
INNER JOIN
  `cbh-db-mirror-prod.member_index_mirror.member_demographics` md
ON md.memberId = m.id
LEFT JOIN
  latest_address address
ON address.memberId = m.id
LEFT JOIN 
  latest_phone phone
ON 
  phone.memberId = m.id
LEFT JOIN
  consented_patients
ON
  consented_patients.patientId = md.memberId
WHERE 
  m.partnerId = 1 AND m.deletedAt is null
  AND md.deletedAt is null