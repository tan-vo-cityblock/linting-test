SELECT DISTINCT
  patient.patientId,
  CASE
    WHEN (claim.POS IN ('21', '51')
      AND claim.UB_BILL_TYPE IN ('0111', '0117'))
    OR (  (claim.REV_CODE LIKE "011%"
           OR claim.REV_CODE LIKE "012%"
           OR claim.REV_CODE LIKE "013%"
           OR claim.REV_CODE LIKE "014%"
           OR claim.REV_CODE LIKE "015%"
           OR claim.REV_CODE LIKE "020%"
           OR claim.REV_CODE IN ("0100", "0101", "0160", "0164",
                                 "0167", "0169", "0170", "0171",
                                 "0173", "0174", "0179", "0190",
                                 "0191", "0192", "0193", "0194",
                                 "0199", "0210", "0211", "0212",
                                 "0213", "0214", "0219", "1000",
                                 "1001", "1002"))
            AND (claim.REV_CODE NOT IN ("0115", "0125", "0135",
                                        "0145", "0155", "0205"))) 
    THEN "inpatient"
    ELSE "other"
  END AS encounter_type,
  claim.ICD_DIAG_01 AS code,
  "icd10" as code_type,
  icd10.name,
  CASE
    WHEN claim.ADM_DATE IS NULL THEN claim.FROM_DATE
    ELSE claim.ADM_DATE
  END AS date,
  "FacilityClaim" as diagnosis_source,
  STRING(NULL) AS data_source,
  claim.SV_line as sv_line,
  claim.CLAIM_ID as claim_id,
  claim.CLAIM_ID as diagnosis_id,
  claim.PROC_CODE as procedure_code,
  claim.REV_CODE as revenue_code,
  claim.UB_BILL_TYPE as ub_bill_type,
  claim.POS as pos,
  claim.DRGCODE AS drg_code,
  claim.ATT_PROV_SPEC AS provider_specialty,
  "emblem" as partner
FROM
  `staging-cityblock-data.monthly_emblem_data.facility` fac
LEFT JOIN
  `reference-data-199919.codesets.icd10cm` icd10
ON
  icd10.code = fac.claim.ICD_DIAG_01
WHERE
  fac.claim.ICD10_OR_HIGHER = TRUE
  AND fac.claim.ICD_DIAG_01 IS NOT NULL
  AND fac.patient.patientId IS NOT NULL
  
UNION ALL

SELECT DISTINCT
  patient.patientId,
  CASE
    WHEN (claim.POS IN ('21', '51')
      AND claim.UB_BILL_TYPE IN ('0111', '0117'))
    OR (  (claim.REV_CODE LIKE "011%"
           OR claim.REV_CODE LIKE "012%"
           OR claim.REV_CODE LIKE "013%"
           OR claim.REV_CODE LIKE "014%"
           OR claim.REV_CODE LIKE "015%"
           OR claim.REV_CODE LIKE "020%"
           OR claim.REV_CODE IN ("0100", "0101", "0160", "0164",
                                 "0167", "0169", "0170", "0171",
                                 "0173", "0174", "0179", "0190",
                                 "0191", "0192", "0193", "0194",
                                 "0199", "0210", "0211", "0212",
                                 "0213", "0214", "0219", "1000",
                                 "1001", "1002"))
            AND (claim.REV_CODE NOT IN ("0115", "0125", "0135",
                                        "0145", "0155", "0205"))) 
    THEN "inpatient"
    ELSE "other"
  END AS encounter_type,
  claim.ICD_DIAG_01 AS code,
  "icd10" as code_type,
  icd10.name,
  CASE
    WHEN claim.ADM_DATE IS NULL THEN claim.FROM_DATE
    ELSE claim.ADM_DATE
  END AS date,
  "ProfessionalClaim" as diagnosis_source,
  STRING(NULL) AS data_source,
  claim.SV_line as sv_line,
  claim.CLAIM_ID as claim_id,
  claim.CLAIM_ID as diagnosis_id,
  claim.PROC_CODE as procedure_code,
  claim.REV_CODE as revenue_code,
  claim.UB_BILL_TYPE as ub_bill_type,
  claim.POS as pos,
  '' AS drg_code,
  claim.ATT_PROV_SPEC AS provider_specialty,
  "emblem" as partner
FROM
  `staging-cityblock-data.monthly_emblem_data.professional` prof
LEFT JOIN
  `reference-data-199919.codesets.icd10cm` icd10
ON
  icd10.code = prof.claim.ICD_DIAG_01
WHERE
  prof.claim.ICD10_OR_HIGHER = TRUE
  AND prof.claim.ICD_DIAG_01 IS NOT NULL
  AND prof.patient.patientId IS NOT NULL
  
UNION ALL

SELECT
  patient.patientId,
  NULL AS encounter_type,
  problem.Code AS code,
  'snomed_ct' AS code_type,
  NULL AS name,
  DATE(TIMESTAMP(problem.StartDate.raw)) AS date,
  'problem_list' AS diagnosis_source,
  LOWER(patient.source.name) AS data_source,
  NULL AS sv_line,
  NULL AS claim_id,
  messageId AS diagnosis_id,
  NULL AS procedure_code,
  NULL AS revenue_code,
  NULL AS ub_bill_type,
  NULL AS pos,
  NULL AS drg_code,
  NULL AS provider_specialty,
  NULL as partner
FROM
  `staging-cityblock-data.medical.patient_problems`
WHERE
  problem.StartDate.raw IS NOT NULL
  
UNION ALL

SELECT
  patient.patientId,
  LOWER(encounter.Type.Name) AS encounter_type,
  diagnoses.Code AS code,
  'snomed_ct' AS code_type,
  NULL AS name,
  DATE(TIMESTAMP(encounter.DateTime.raw)) AS date,
  'ccd' AS diagnosis_source,
  LOWER(patient.source.name) AS data_source,
  NULL AS sv_line,
  NULL AS claim_id,
  messageId AS diagnosis_id,
  NULL AS procedure_code,
  NULL AS revenue_code,
  NULL AS ub_bill_type,
  NULL AS pos,
  NULL AS drg_code,
  NULL AS provider_specialty,
  NULL as partner
FROM `staging-cityblock-data.medical.patient_encounters`,
UNNEST(encounter.Diagnosis) AS diagnoses

UNION ALL

SELECT
  patient.patientId,
  LOWER(eventType) AS encounter_type,
  REGEXP_REPLACE(diagnoses.code, r"[.]", "") AS code,
  diagnoses.codeset AS code_type,
  NULL AS name,
  DATE(TIMESTAMP(dischargeDateTime.raw)) AS date,
  'hie' AS diagnosis_source,
  patient.source.name AS data_source,
  NULL AS sv_line,
  NULL AS claim_id,
  messageId AS diagnosis_id,  
  NULL AS procedure_code,
  NULL AS revenue_code,
  NULL AS ub_bill_type,
  NULL AS pos,
  NULL AS drg_code,
  NULL AS provider_specialty,
  NULL as partner  
FROM `staging-cityblock-data.medical.patient_hie_events`,
UNNEST(diagnoses) AS diagnoses
WHERE patient.patientId IS NOT NULL