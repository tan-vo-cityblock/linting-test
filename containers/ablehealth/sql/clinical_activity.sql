WITH
  final AS (
  SELECT
    p.memberIdentifier.patientId AS patient_id,
    CASE
      WHEN vsd.value_set_name IN ('Diabetic Retinal Screening', 'Diabetic Retinal Screening With Eye Care Professional', 'Diabetic Retinal Screening Negative', 'Unilateral Eye Enucleation%') AND lines.procedure.code NOT IN ('99203', '99204', '99205', '99213', '99214', '99215', '99242', '99243', '99244', '99245') THEN '100'
    ELSE
    ""
  END
    AS provider_id,
    p.header.partnerClaimId AS claim_id,
    lines.procedure.codeset AS code_type,
    lines.procedure.code AS code_value,
    "" AS result,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(lines.date.FROM AS TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(lines.date.TO AS TIMESTAMP)) AS stop_datetime,
    lines.placeOfService AS place_of_service,
    case when lines.procedure.tier = 'secondary' then 0 else 1 end as principal
  FROM
    `connecticare-data.gold_claims.Professional` AS p
  CROSS JOIN
    UNNEST(p.lines) AS lines
  LEFT JOIN
    `reference-data-199919.hedis.vsd_value_set_to_code_2020` vsd
  ON
    vsd.code = lines.procedure.code
  WHERE
    p.memberIdentifier.patientId IS NOT NULL

  UNION DISTINCT
    --Emblem Professional
    
  SELECT
    p.memberIdentifier.patientId AS patient_id,
    CASE
      WHEN vsd.value_set_name IN ('Diabetic Retinal Screening', 'Diabetic Retinal Screening With Eye Care Professional', 'Diabetic Retinal Screening Negative', 'Unilateral Eye Enucleation%') AND lines.procedure.code NOT IN ('99203', '99204', '99205', '99213', '99214', '99215', '99242', '99243', '99244', '99245') THEN '100'
    ELSE
    ""
  END
    AS provider_id,
    p.header.partnerClaimId AS claim_id,
    lines.procedure.codeset AS code_type,
    lines.procedure.code AS code_value,
    "" AS result,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(lines.date.FROM AS TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(lines.date.TO AS TIMESTAMP)) AS stop_datetime,
    lines.placeOfService AS place_of_service,
    case when lines.procedure.tier = 'secondary' then 0 else 1 end as principal
  FROM
    `emblem-data.gold_claims.Professional` AS p
  CROSS JOIN
    UNNEST(p.lines) AS lines
  LEFT JOIN
    `reference-data-199919.hedis.vsd_value_set_to_code_2020` vsd
  ON
    vsd.code = lines.procedure.code
  WHERE
    p.memberIdentifier.patientId IS NOT NULL
    
    UNION DISTINCT
    
    --healthyblue
    SELECT
    p.memberIdentifier.patientId AS patient_id,
    CASE
      WHEN vsd.value_set_name IN ('Diabetic Retinal Screening', 'Diabetic Retinal Screening With Eye Care Professional', 'Diabetic Retinal Screening Negative', 'Unilateral Eye Enucleation%') AND lines.procedure.code NOT IN ('99203', '99204', '99205', '99213', '99214', '99215', '99242', '99243', '99244', '99245') THEN '100'
    ELSE
    ""
  END
    AS provider_id,
    p.header.partnerClaimId AS claim_id,
    lines.procedure.codeset AS code_type,
    lines.procedure.code AS code_value,
    "" AS result,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(lines.date.FROM AS TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(lines.date.TO AS TIMESTAMP)) AS stop_datetime,
    lines.placeOfService AS place_of_service,
    case when lines.procedure.tier = 'secondary' then 0 else 1 end as principal
  FROM
    `cbh-healthyblue-data.gold_claims.Professional` AS p
  CROSS JOIN
    UNNEST(p.lines) AS lines
  LEFT JOIN
    `reference-data-199919.hedis.vsd_value_set_to_code_2020` vsd
  ON
    vsd.code = lines.procedure.code
  WHERE
    p.memberIdentifier.patientId IS NOT NULL
    
    
  UNION DISTINCT
    --Emblem Facility
  SELECT
    p.memberIdentifier.patientId AS patient_id,
    CASE
      WHEN vsd.value_set_name IN ('Diabetic Retinal Screening', 'Diabetic Retinal Screening With Eye Care Professional', 'Diabetic Retinal Screening Negative', 'Unilateral Eye Enucleation%') AND lines.procedure.code NOT IN ('99203', '99204', '99205', '99213', '99214', '99215', '99242', '99243', '99244', '99245') THEN '100'
    ELSE
    ""
  END
    AS provider_id,
    p.header.partnerClaimId AS claim_id,
    lines.procedure.codeset AS code_type,
    lines.procedure.code AS code_value,
    "" AS result,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.FROM AS TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.TO AS TIMESTAMP)) AS stop_datetime,
    "" AS place_of_service,
    case when lines.procedure.tier = 'secondary' then 0 else 1 end as principal
  FROM
    `emblem-data.gold_claims.Facility` AS p
  CROSS JOIN
    UNNEST(p.lines) AS lines
  LEFT JOIN
    `reference-data-199919.hedis.vsd_value_set_to_code_2020` vsd
  ON
    vsd.code = lines.procedure.code
  WHERE
    p.memberIdentifier.patientId IS NOT NULL
    
  UNION DISTINCT
    
    --CCI Facility
  SELECT
    p.memberIdentifier.patientId AS patient_id,
    CASE
      WHEN vsd.value_set_name IN ('Diabetic Retinal Screening', 'Diabetic Retinal Screening With Eye Care Professional', 'Diabetic Retinal Screening Negative', 'Unilateral Eye Enucleation%') AND lines.procedure.code NOT IN ('99203', '99204', '99205', '99213', '99214', '99215', '99242', '99243', '99244', '99245') THEN '100'
    ELSE
    ""
  END
    AS provider_id,
    p.header.partnerClaimId AS claim_id,
    lines.procedure.codeset AS code_type,
    lines.procedure.code AS code_value,
    "" AS result,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.FROM AS TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.TO AS TIMESTAMP)) AS stop_datetime,
    "" AS place_of_service,
    case when lines.procedure.tier = 'secondary' then 0 else 1 end as principal
  FROM
    `connecticare-data.gold_claims.Facility` AS p
  CROSS JOIN
    UNNEST(p.lines) AS lines
  LEFT JOIN
    `reference-data-199919.hedis.vsd_value_set_to_code_2020` vsd
  ON
    vsd.code = lines.procedure.code
  WHERE
    p.memberIdentifier.patientId IS NOT NULL
    
 
UNION DISTINCT
  
    --Tufts Professional
  SELECT
    p.memberIdentifier.patientId AS patient_id,
    CASE
      WHEN vsd.value_set_name IN ('Diabetic Retinal Screening', 'Diabetic Retinal Screening With Eye Care Professional', 'Diabetic Retinal Screening Negative', 'Unilateral Eye Enucleation%') AND lines.procedure.code NOT IN ('99203', '99204', '99205', '99213', '99214', '99215', '99242', '99243', '99244', '99245') THEN '100'
    ELSE
    ""
  END
    AS provider_id,
    p.header.partnerClaimId AS claim_id,
    lines.procedure.codeset AS code_type,
    lines.procedure.code AS code_value,
    "" AS result,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(lines.date.FROM AS TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(lines.date.TO AS TIMESTAMP)) AS stop_datetime,
    lines.placeOfService AS place_of_service,
    case when lines.procedure.tier = 'secondary' then 0 else 1 end as principal
  FROM
    `tufts-data.gold_claims.Professional_*` AS p
  CROSS JOIN
    UNNEST(p.lines) AS lines
  LEFT JOIN
    `reference-data-199919.hedis.vsd_value_set_to_code_2020` vsd
  ON
    vsd.code = lines.procedure.code
  WHERE
    p.memberIdentifier.patientId IS NOT NULL
                                            
  UNION DISTINCT

--Tufts Facility
  SELECT
    p.memberIdentifier.patientId AS patient_id,
    CASE
      WHEN vsd.value_set_name IN ('Diabetic Retinal Screening', 'Diabetic Retinal Screening With Eye Care Professional', 'Diabetic Retinal Screening Negative', 'Unilateral Eye Enucleation%') AND lines.procedure.code NOT IN ('99203', '99204', '99205', '99213', '99214', '99215', '99242', '99243', '99244', '99245') THEN '100'
    ELSE
    ""
  END
    AS provider_id,
    p.header.partnerClaimId AS claim_id,
    lines.procedure.codeset AS code_type,
    lines.procedure.code AS code_value,
    "" AS result,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.FROM AS TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.TO AS TIMESTAMP)) AS stop_datetime,
    "" AS place_of_service,
    case when lines.procedure.tier = 'secondary' then 0 else 1 end as principal
  FROM
    `tufts-data.gold_claims.Facility` AS p
  CROSS JOIN
    UNNEST(p.lines) AS lines
  LEFT JOIN
    `reference-data-199919.hedis.vsd_value_set_to_code_2020` vsd
  ON
    vsd.code = lines.procedure.code
  WHERE
    p.memberIdentifier.patientId IS NOT NULL
  
  UNION DISTINCT
                                             
  --CareFirst Professional
  SELECT
    p.memberIdentifier.patientId AS patient_id,
    CASE
      WHEN vsd.value_set_name IN ('Diabetic Retinal Screening', 'Diabetic Retinal Screening With Eye Care Professional', 'Diabetic Retinal Screening Negative', 'Unilateral Eye Enucleation%') AND lines.procedure.code NOT IN ('99203', '99204', '99205', '99213', '99214', '99215', '99242', '99243', '99244', '99245') THEN '100'
    ELSE
    ""
  END
    AS provider_id,
    p.header.partnerClaimId AS claim_id,
    lines.procedure.codeset AS code_type,
    lines.procedure.code AS code_value,
    "" AS result,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(lines.date.FROM AS TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(lines.date.TO AS TIMESTAMP)) AS stop_datetime,
    lines.placeOfService AS place_of_service,
    case when lines.procedure.tier = 'secondary' then 0 else 1 end as principal
  FROM
    `cbh-carefirst-data.gold_claims.Professional_*` AS p
  CROSS JOIN
    UNNEST(p.lines) AS lines
  LEFT JOIN
    `reference-data-199919.hedis.vsd_value_set_to_code_2020` vsd
  ON
    vsd.code = lines.procedure.code
  WHERE
    p.memberIdentifier.patientId IS NOT NULL
                                            
UNION DISTINCT
                                            
--CareFirst Facility
  SELECT
    p.memberIdentifier.patientId AS patient_id,
    CASE
      WHEN vsd.value_set_name IN ('Diabetic Retinal Screening', 'Diabetic Retinal Screening With Eye Care Professional', 'Diabetic Retinal Screening Negative', 'Unilateral Eye Enucleation%') AND lines.procedure.code NOT IN ('99203', '99204', '99205', '99213', '99214', '99215', '99242', '99243', '99244', '99245') THEN '100'
    ELSE
    ""
  END
    AS provider_id,
    p.header.partnerClaimId AS claim_id,
    lines.procedure.codeset AS code_type,
    lines.procedure.code AS code_value,
    "" AS result,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.FROM AS TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.TO AS TIMESTAMP)) AS stop_datetime,
    "" AS place_of_service,
    case when lines.procedure.tier = 'secondary' then 0 else 1 end as principal
  FROM
    `cbh-carefirst-data.gold_claims.Facility` AS p
  CROSS JOIN
    UNNEST(p.lines) AS lines
  LEFT JOIN
    `reference-data-199919.hedis.vsd_value_set_to_code_2020` vsd
  ON
    vsd.code = lines.procedure.code
  WHERE
    p.memberIdentifier.patientId IS NOT NULL
  
  UNION DISTINCT
                                            
 --EHR Vital Signs
  SELECT
    pvs.patient.patientId AS patient_id,
    "" AS provider_id,
    "" AS claim_id,
    CASE
      WHEN pvs.vitalSign.codeSystemName = "LOINC" THEN "LOINC"
    ELSE
    "NOTMAPPED"
  END
    AS code_type,
    pvs.vitalSign.Code AS code_value,
    pvs.vitalSign.Value AS result,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(pvs.vitalSign.DateTime.raw AS TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(pvs.vitalSign.DateTime.raw AS TIMESTAMP)) AS stop_datetime,
    "" AS place_of_service,
     1  as principal
     from
    `cityblock-data.medical.patient_vital_signs` AS pvs
           
      UNION DISTINCT                                       
    -- REVCODE ADDITIONS
                                            
    SELECT
    p.memberIdentifier.patientId AS patient_id,
    case when lines.revenueCode is not null then '100'  else '' end AS provider_id,
    p.header.partnerClaimId AS claim_id,
    'UBREV' AS code_type,
    LPAD(lines.revenueCode ,4,"0") AS code_value,
    "" AS result,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.FROM AS TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.TO AS TIMESTAMP)) AS stop_datetime,
    "" AS place_of_service,
    case when lines.procedure.tier = 'secondary' then 0 else 1 end as principal
  FROM
    `tufts-data.gold_claims.Facility` AS p
  CROSS JOIN
    UNNEST(p.lines) AS lines
    left join 
    `reference-data-199919.hedis.hedis_2018_UB_codes`
    on lines.revenueCode = code_value
  WHERE
    p.memberIdentifier.patientId IS NOT NULL
    and lines.revenueCode is not null
    
  union distinct 
  
   SELECT
    p.memberIdentifier.patientId AS patient_id,
    case when lines.revenueCode is not null then '100'  else '' end AS provider_id,
    p.header.partnerClaimId AS claim_id,
    'UBREV' AS code_type,
    LPAD(lines.revenueCode ,4,"0") AS code_value,
    "" AS result,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.FROM AS TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.TO AS TIMESTAMP)) AS stop_datetime,
    "" AS place_of_service,
    case when lines.procedure.tier = 'secondary' then 0 else 1 end as principal
  FROM
    `emblem-data.gold_claims.Facility` AS p
  CROSS JOIN
    UNNEST(p.lines) AS lines
        left join 
    `reference-data-199919.hedis.hedis_2018_UB_codes`
    on lines.revenueCode = code_value
  WHERE
    p.memberIdentifier.patientId IS NOT NULL
    and lines.revenueCode is not null
 
 union distinct 
  
   SELECT
    p.memberIdentifier.patientId AS patient_id,
    case when lines.revenueCode is not null then '100'  else '' end AS provider_id,
    p.header.partnerClaimId AS claim_id,
    'UBREV' AS code_type,
    LPAD(lines.revenueCode ,4,"0") AS code_value,
    "" AS result,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.FROM AS TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.TO AS TIMESTAMP)) AS stop_datetime,
    "" AS place_of_service,
    case when lines.procedure.tier = 'secondary' then 0 else 1 end as principal
  FROM
    `connecticare-data.gold_claims.Facility` AS p
  CROSS JOIN
    UNNEST(p.lines) AS lines
        left join 
    `reference-data-199919.hedis.hedis_2018_UB_codes`
    on lines.revenueCode = code_value
  WHERE
    p.memberIdentifier.patientId IS NOT NULL
    and lines.revenueCode is not null
    
  union distinct 
  
   SELECT
    p.memberIdentifier.patientId AS patient_id,
    case when lines.revenueCode is not null then '100'  else '' end AS provider_id,
    p.header.partnerClaimId AS claim_id,
    'UBREV' AS code_type,
    LPAD(lines.revenueCode ,4,"0") AS code_value,
    "" AS result,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.FROM AS TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.TO AS TIMESTAMP)) AS stop_datetime,
    "" AS place_of_service,
    case when lines.procedure.tier = 'secondary' then 0 else 1 end as principal
  FROM
    `cbh-healthyblue-data.gold_claims.Facility` AS p
  CROSS JOIN
    UNNEST(p.lines) AS lines
        left join 
    `reference-data-199919.hedis.hedis_2018_UB_codes`
    on lines.revenueCode = code_value
  WHERE
    p.memberIdentifier.patientId IS NOT NULL
    and lines.revenueCode is not null
    
 union distinct 
  
   SELECT
    p.memberIdentifier.patientId AS patient_id,
    case when lines.revenueCode is not null then '100'  else '' end AS provider_id,
    p.header.partnerClaimId AS claim_id,
    'UBREV' AS code_type,
    LPAD(lines.revenueCode ,4,"0") AS code_value,
    "" AS result,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.FROM AS TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(header.date.TO AS TIMESTAMP)) AS stop_datetime,
    "" AS place_of_service,
    case when lines.procedure.tier = 'secondary' then 0 else 1 end as principal
  FROM
    `cbh-carefirst-data.gold_claims.Facility` AS p
  CROSS JOIN
    UNNEST(p.lines) AS lines
        left join 
    `reference-data-199919.hedis.hedis_2018_UB_codes`
    on lines.revenueCode = code_value
  WHERE
    p.memberIdentifier.patientId IS NOT NULL
    and lines.revenueCode is not null    
                                            
union distinct 

SELECT
    patient.patientId AS patient_id,
     '' AS provider_id,
    unenc.id AS claim_id,
    'CPT' AS code_type,
    encounter.type.code AS code_value,
    "" AS result,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(encounter.dateTime.instant AS TIMESTAMP)) AS start_datetime,
    FORMAT_TIMESTAMP("%Y-%m-%dT%X%Ez", CAST(encounter.dateTime.instant AS TIMESTAMP)) AS stop_datetime,
    case when lower(encounter.type.name) in ('telehealth', 'telemedicine') then "02"  
    when lower(encounter.type.name) = 'home care visit' then "12"
    when lower(encounter.type.name) like "%office visit%" then "11" 
    when lower(encounter.type.name) = "visit note" then "11" 
    else ""
    end AS place_of_service,
     1 as principal    
FROM `cityblock-data.dev_medical.patient_encounters` ,
unnest(encounter.identifiers) unenc
where upper(encounter.type.codeSystemName) like '%CPT%'
                                           )
SELECT
  *
FROM
  final
