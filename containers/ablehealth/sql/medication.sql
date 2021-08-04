SELECT
  pm.patient.patientId as patient_id,
  "" AS provider_id,
  CASE
    WHEN pm.medication.Product.CodeSystemName = "NDC" THEN "NDC"
    WHEN pm.medication.Product.CodeSystemName = "RxNorm" THEN "RXNORM"
    WHEN pm.medication.Product.CodeSystemName = "RXNORM" THEN "RXNORM"
    ELSE "NOTMAPPED"
  END AS code_type,
  pm.medication.Product.Code as code_value,
  CASE
    WHEN LENGTH(pm.medication.StartDate.raw) = 8 THEN FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", PARSE_TIMESTAMP("%Y%m%d", pm.medication.StartDate.raw))
    ELSE FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(pm.medication.StartDate.raw as TIMESTAMP))
  END AS start_date,
  CASE
    WHEN LENGTH(pm.medication.EndDate.raw) = 8 THEN FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", PARSE_TIMESTAMP("%Y%m%d", pm.medication.EndDate.raw))
    ELSE FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(pm.medication.EndDate.raw as TIMESTAMP))
  END AS end_date,
  NULL AS prescribed,
  NULL AS dispensed,
  CONCAT(pm.medication.Dose.Quantity,' ',pm.medication.Frequency.Unit) as dose,
  NULL AS frequency,
  NULL AS days_supplied,
  NULL AS quantity,
  pm.medication.Route.Name AS route,
  NULL AS pharmacy_npi,
  "patient medications" AS source
FROM
  `cityblock-data.medical.patient_medications` pm

UNION DISTINCT

SELECT
  ep.memberIdentifier.patientId,
  ep.prescriber.id AS provider_NPI,
  "NDC" AS code_type,
  ep.drug.NDC as code_value,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(date.filled as TIMESTAMP)) AS start_date,
  "" AS end_date,
  CASE 
    WHEN prescriber.NPI IS NOT NULL THEN 1
    ELSE NULL
    END AS prescribed,
  CASE 
    WHEN drug.quantityDispensed IS NOT NULL THEN 1
    ELSE NULL
    END AS dispensed,
  NULL AS dose,
  NULL AS frequency, 
  drug.daysSupply AS days_supplied,
  drug.quantityDispensed AS quantity,
  NULL AS route,
  ep.pharmacy.NPI AS pharmacy_npi,
  "gold_pharmacy_emblem" AS source
FROM
  `emblem-data.gold_claims.Pharmacy` ep
WHERE ep. memberIdentifier.patientId IS NOT NULL

UNION DISTINCT

SELECT
  ep.memberIdentifier.patientId,
  ep.prescriber.id AS provider_NPI,
  "NDC" AS code_type,
  ep.drug.NDC as code_value,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(date.filled as TIMESTAMP)) AS start_date,
  "" AS end_date,
  CASE 
    WHEN prescriber.NPI IS NOT NULL THEN 1
    ELSE NULL
    END AS prescribed, # CONFIRM LOGIC
  CASE 
    WHEN drug.quantityDispensed IS NOT NULL THEN 1
    ELSE NULL
    END AS dispensed,
  NULL AS dose,
  NULL AS frequency, 
  drug.daysSupply AS days_supplied,
  drug.quantityDispensed AS quantity,
  NULL AS route,
  ep.pharmacy.NPI AS pharmacy_npi,
  "gold_pharmacy_connecticare" AS source
FROM
  `connecticare-data.gold_claims.Pharmacy` ep
WHERE ep. memberIdentifier.patientId IS NOT NULL

UNION DISTINCT

SELECT
  ep.memberIdentifier.patientId,
  ep.prescriber.id AS provider_NPI,
  "NDC" AS code_type,
  ep.drug.NDC as code_value,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(date.filled as TIMESTAMP)) AS start_date,
  "" AS end_date,
  CASE 
    WHEN prescriber.NPI IS NOT NULL THEN 1
    ELSE NULL
    END AS prescribed, # CONFIRM LOGIC
  CASE 
    WHEN drug.quantityDispensed IS NOT NULL THEN 1
    ELSE NULL
    END AS dispensed,
  NULL AS dose,
  NULL AS frequency, 
  drug.daysSupply AS days_supplied,
  drug.quantityDispensed AS quantity,
  NULL AS route,
  ep.pharmacy.NPI AS pharmacy_npi,
  "gold_pharmacy_tufts" AS source
FROM
  `tufts-data.gold_claims.Pharmacy_*` ep
WHERE ep. memberIdentifier.patientId IS NOT NULL

UNION DISTINCT

SELECT
  ep.memberIdentifier.patientId,
  ep.prescriber.id AS provider_NPI,
  "NDC" AS code_type,
  ep.drug.NDC as code_value,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(date.filled as TIMESTAMP)) AS start_date,
  "" AS end_date,
  CASE 
    WHEN prescriber.NPI IS NOT NULL THEN 1
    ELSE NULL
    END AS prescribed, # CONFIRM LOGIC
  CASE 
    WHEN drug.quantityDispensed IS NOT NULL THEN 1
    ELSE NULL
    END AS dispensed,
  NULL AS dose,
  NULL AS frequency, 
  drug.daysSupply AS days_supplied,
  drug.quantityDispensed AS quantity,
  NULL AS route,
  ep.pharmacy.NPI AS pharmacy_npi,
  "gold_pharmacy_tufts" AS source
FROM
  `cbh-carefirst-data.gold_claims.Pharmacy_*` ep
WHERE ep. memberIdentifier.patientId IS NOT NULL

UNION DISTINCT

SELECT
  ep.memberIdentifier.patientId,
  ep.prescriber.id AS provider_NPI,
  "NDC" AS code_type,
  ep.drug.NDC as code_value,
  FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(date.filled as TIMESTAMP)) AS start_date,
  "" AS end_date,
  CASE 
    WHEN prescriber.NPI IS NOT NULL THEN 1
    ELSE NULL
    END AS prescribed, # CONFIRM LOGIC
  CASE 
    WHEN drug.quantityDispensed IS NOT NULL THEN 1
    ELSE NULL
    END AS dispensed,
  NULL AS dose,
  NULL AS frequency, 
  drug.daysSupply AS days_supplied,
  drug.quantityDispensed AS quantity,
  NULL AS route,
  ep.pharmacy.NPI AS pharmacy_npi,
  "gold_pharmacy_tufts" AS source
FROM
  `cbh-cardinal-data.gold_claims.Pharmacy_*` ep
WHERE ep. memberIdentifier.patientId IS NOT NULL
