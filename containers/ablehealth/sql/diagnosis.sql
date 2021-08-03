SELECT
  f.memberIdentifier.patientId AS patient_id,
  f.header.partnerClaimId AS claim_id,
  facility_diags.codeset AS code_type,
  facility_diags.code AS code_value,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(f.header.date.admit as TIMESTAMP)) AS start_datetime,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(f.header.date.discharge as TIMESTAMP)) AS stop_datetime
FROM
  `connecticare-data.gold_claims.Facility` f
CROSS JOIN
  UNNEST(f.header.diagnoses) AS facility_diags
WHERE
  f.memberIdentifier.patientId IS NOT NULL

UNION DISTINCT

SELECT
  f.memberIdentifier.patientId AS patient_id,
  f.header.partnerClaimId AS claim_id,
  facility_diags.codeset AS code_type,
  facility_diags.code AS code_value,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(f.header.date.admit as TIMESTAMP)) AS start_datetime,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(f.header.date.discharge as TIMESTAMP)) AS stop_datetime
FROM
  `emblem-data.gold_claims.Facility` f
CROSS JOIN
  UNNEST(f.header.diagnoses) AS facility_diags
WHERE
  f.memberIdentifier.patientId IS NOT NULL

UNION DISTINCT

SELECT
  p.memberIdentifier.patientId AS patient_id,
  p.header.partnerClaimId AS claim_id,
  prof_diags.codeset AS code_type,
  prof_diags.code AS code_value,
  (
  SELECT
    AS STRUCT
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(MAX(date.from) as TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(MAX(date.to) as TIMESTAMP)) AS stop_datetime
  FROM
    UNNEST(p.lines)).*
FROM
  `connecticare-data.gold_claims.Professional` p
CROSS JOIN
  UNNEST(p.header.diagnoses) AS prof_diags
WHERE
  p.memberIdentifier.patientId IS NOT NULL
  
UNION DISTINCT

SELECT
  p.memberIdentifier.patientId AS patient_id,
  p.header.partnerClaimId AS claim_id,
  prof_diags.codeset AS code_type,
  prof_diags.code AS code_value,
  (
  SELECT
    AS STRUCT
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(MAX(date.from) as TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(MAX(date.to) as TIMESTAMP)) AS stop_datetime
  FROM
    UNNEST(p.lines)).*
FROM
  `emblem-data.gold_claims.Professional` p
CROSS JOIN
  UNNEST(p.header.diagnoses) AS prof_diags
WHERE
  p.memberIdentifier.patientId IS NOT NULL
  
UNION DISTINCT

SELECT
  p.memberIdentifier.patientId AS patient_id,
  p.header.partnerClaimId AS claim_id,
  prof_diags.codeset AS code_type,
  prof_diags.code AS code_value,
  (
  SELECT
    AS STRUCT
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(MAX(date.from) as TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(MAX(date.to) as TIMESTAMP)) AS stop_datetime
  FROM
    UNNEST(p.lines)).*
FROM
  `cbh-cardinal-data.gold_claims.Professional` p
CROSS JOIN
  UNNEST(p.header.diagnoses) AS prof_diags
WHERE
  p.memberIdentifier.patientId IS NOT NULL
  
UNION DISTINCT

SELECT
  p.memberIdentifier.patientId AS patient_id,
  p.header.partnerClaimId AS claim_id,
  prof_diags.codeset AS code_type,
  prof_diags.code AS code_value,
  (
  SELECT
    AS STRUCT
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(MAX(date.from) as TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(MAX(date.to) as TIMESTAMP)) AS stop_datetime
  FROM
    UNNEST(p.lines)).*
FROM
  `cbh-carefirst-data.gold_claims.Professional` p
CROSS JOIN
  UNNEST(p.header.diagnoses) AS prof_diags
WHERE
  p.memberIdentifier.patientId IS NOT NULL

UNION DISTINCT

SELECT
  f.memberIdentifier.patientId AS patient_id,
  f.header.partnerClaimId AS claim_id,
  facility_diags.codeset AS code_type,
  facility_diags.code AS code_value,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(f.header.date.admit as TIMESTAMP)) AS start_datetime,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(f.header.date.discharge as TIMESTAMP)) AS stop_datetime
FROM
  `cbh-carefirst-data.gold_claims.Facility` f
CROSS JOIN
  UNNEST(f.header.diagnoses) AS facility_diags
WHERE
  f.memberIdentifier.patientId IS NOT NULL

UNION DISTINCT

SELECT
  f.memberIdentifier.patientId AS patient_id,
  f.header.partnerClaimId AS claim_id,
  facility_diags.codeset AS code_type,
  facility_diags.code AS code_value,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(f.header.date.admit as TIMESTAMP)) AS start_datetime,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(f.header.date.discharge as TIMESTAMP)) AS stop_datetime
FROM
  `cbh-cardinal-data.gold_claims.Facility` f
CROSS JOIN
  UNNEST(f.header.diagnoses) AS facility_diags
WHERE
  f.memberIdentifier.patientId IS NOT NULL

UNION DISTINCT
           
SELECT
  p.memberIdentifier.patientId AS patient_id,
  p.header.partnerClaimId AS claim_id,
  prof_diags.codeset AS code_type,
  prof_diags.code AS code_value,
  (
  SELECT
    AS STRUCT
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(MAX(date.from) as TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(MAX(date.to) as TIMESTAMP)) AS stop_datetime
  FROM
    UNNEST(p.lines)).*
FROM
  `tufts-data.gold_claims.Professional` p
CROSS JOIN
  UNNEST(p.header.diagnoses) AS prof_diags
WHERE
  p.memberIdentifier.patientId IS NOT NULL
  
UNION DISTINCT

SELECT
  f.memberIdentifier.patientId AS patient_id,
  f.header.partnerClaimId AS claim_id,
  facility_diags.codeset AS code_type,
  facility_diags.code AS code_value,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(f.header.date.admit as TIMESTAMP)) AS start_datetime,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(f.header.date.discharge as TIMESTAMP)) AS stop_datetime
FROM
  `tufts-data.gold_claims.Facility` f
CROSS JOIN
  UNNEST(f.header.diagnoses) AS facility_diags
WHERE
  f.memberIdentifier.patientId IS NOT NULL
  
  UNION DISTINCT
  
  SELECT
  p.memberIdentifier.patientId AS patient_id,
  p.header.partnerClaimId AS claim_id,
  prof_diags.codeset AS code_type,
  prof_diags.code AS code_value,
  (
  SELECT
    AS STRUCT
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(MAX(date.from) as TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(MAX(date.to) as TIMESTAMP)) AS stop_datetime
  FROM
    UNNEST(p.lines)).*
FROM
  `cbh-healthyblue-data.gold_claims.Professional` p
CROSS JOIN
  UNNEST(p.header.diagnoses) AS prof_diags
WHERE
  p.memberIdentifier.patientId IS NOT NULL
  
UNION DISTINCT

SELECT
  f.memberIdentifier.patientId AS patient_id,
  f.header.partnerClaimId AS claim_id,
  facility_diags.codeset AS code_type,
  facility_diags.code AS code_value,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(f.header.date.admit as TIMESTAMP)) AS start_datetime,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(f.header.date.discharge as TIMESTAMP)) AS stop_datetime
FROM
  `cbh-healthyblue-data.gold_claims.Facility` f
CROSS JOIN
  UNNEST(f.header.diagnoses) AS facility_diags
WHERE
  f.memberIdentifier.patientId IS NOT NULL
