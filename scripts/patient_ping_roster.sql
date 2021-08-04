WITH
  patient_roster AS (
  SELECT
    p.id AS PATIENT_ID,
    p.firstName AS PATIENT_FIRST_NAME,
    p.lastName AS PATIENT_LAST_NAME,
    p.dateOfBirth AS PATIENT_DOB,
    CASE
      WHEN pi.gender IS NULL THEN 'U'
    ELSE
      UPPER(pi.gender)
    END AS PATIENT_GENDER,
    a.zip AS PATIENT_ADDRESS_ZIP,
    CASE
      -- MA Market ID
      WHEN p.homeMarketId = '76150da6-b62a-4099-9bd6-42e7be3ffc62' THEN 'Cityblock Health MA'
      WHEN u.id IS NULL THEN 'Cityblock Health'
    ELSE
      CONCAT('Cityblock', ' - ', TRIM(u.firstName), ' ', TRIM(u.lastName))
    END AS PRACTICE_NAME_1,
    CASE
      WHEN p.homeMarketId = '9065e1fc-2659-4828-9519-3ad49abf5126' THEN 'Cityblock Medical Practice CT'
      WHEN p.homeMarketId = '76150da6-b62a-4099-9bd6-42e7be3ffc62' THEN 'Cityblock Medical Practice MA'
      WHEN p.homeMarketId = '31c505ee-1e1b-4f5c-8e3e-4a2bc9937e04' THEN 'Cityblock Medical Practice NC'
    ELSE
      'Cityblock Medical Practice'
    END
      AS PROGRAM_1,
    RANK() OVER (PARTITION BY p.id ORDER BY ct.isPrimaryContact desc, CASE u.userRole WHEN 'Community_Health_Partner' THEN 1 WHEN 'Lead_Community_Health_Partner' THEN 2 WHEN 'Outreach_Specialist' THEN 3 ELSE 4 END , ct.createdAt DESC) AS rank
  FROM
    `commons_mirror.patient` p
  JOIN
    `commons_mirror.patient_info` pi
  ON
    p.id = pi.patientId
  JOIN
    `commons_mirror.address` a
  ON
    pi.primaryAddressId = a.id
  JOIN
    `commons_mirror.patient_state` ps
  ON
    p.id = ps.patientId
  LEFT OUTER JOIN
    `commons_mirror.care_team` ct
  ON
    ct.patientId = p.id
    AND ct.deletedAt IS NULL
  LEFT OUTER JOIN
    `commons_mirror.user` u
  ON
    ct.userId = u.id
    AND u.deletedAt IS NULL
  WHERE
    -- Include Market IDs
    p.homeMarketId IN (
      '9065e1fc-2659-4828-9519-3ad49abf5126',
      '76150da6-b62a-4099-9bd6-42e7be3ffc62',
      '31c505ee-1e1b-4f5c-8e3e-4a2bc9937e04'
      )
    AND ps.deletedAt IS NULL
    AND ps.currentState != 'disenrolled')
SELECT
  PATIENT_ID,
  PATIENT_FIRST_NAME,
  PATIENT_LAST_NAME,
  PATIENT_DOB,
  PATIENT_GENDER,
  PATIENT_ADDRESS_ZIP,
  PRACTICE_NAME_1,
  PROGRAM_1
FROM
  patient_roster
WHERE
  patient_roster.rank = 1
