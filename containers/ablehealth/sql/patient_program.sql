SELECT  distinct
  id AS patient_id,
  --insurance, 
  --lineOfBusiness, productDescription,
  CASE 
    WHEN lower(insurance) = 'cardinal' AND lower(lineOfBusiness) = 'medicaid' THEN 'Cardinal Medicaid'
    WHEN lower(insurance) = 'connecticare' AND lower(lineOfBusiness) IN ('medicare','m') THEN 'ConnectiCare Medicare'
    WHEN lower(insurance) = 'connecticare' AND lower(lineOfBusiness) = 'dsnp' THEN 'ConnectiCare DSNP'
    WHEN lower(insurance) = 'connecticare' AND (productDescription ='fully insured' or productDescription = 'Commercial - Fully Insured' OR productDescription = 'Commercial - Fully Insured Off Exchange') THEN 'ConnectiCare Commercial - Fully Insured'
    WHEN lower(insurance) = 'connecticare' AND productDescription = 'Commercial - Exchange' THEN 'ConnectiCare Commercial - Exchange'
    WHEN lower(insurance) like '%emblem%' AND lower(lineOfBusiness) = 'medicare' THEN 'Emblem Medicare'
    WHEN lower(insurance) like '%emblem%' AND lower(lineOfBusiness) = 'dsnp' THEN 'Emblem DSNP'
    WHEN lower(insurance) like '%emblem%' AND lower(lineOfBusiness) = 'medicaid' THEN 'Emblem Medicaid'
    WHEN lower(insurance) like '%emblem%' AND lower(lineOfBusiness) IN ('hmo','ps','commercial') THEN 'Emblem Commercial'
    WHEN lower(insurance) = 'carefirst' AND lower(lineOfBusiness) = 'medicaid' THEN 'Carefirst Medicaid'
    WHEN lower(insurance) = 'tufts' THEN 'Tufts One Care'
    WHEN lower(insurance) = 'bcbsnc' AND lower(lineOfBusiness) = 'medicaid' THEN 'BCBS NC Medicaid'
    WHEN lower(insurance) = 'medicarenc' AND lower(lineOfBusiness) = 'medicareffs' THEN 'BCBS NC Medicare FFS'
    WHEN lower(insurance) = 'bcbsnc' AND lower(lineOfBusiness) = 'bluecrossmedicareadvantage' THEN 'BCBS NC Medicare Advantage'
    WHEN lower(insurance) = 'bcbsnc' AND lower(lineOfBusiness) = 'medicaidffs' THEN 'BCBS NC Medicaid FFS'
    WHEN lower(insurance) = 'medicaidnc' AND lower(lineOfBusiness) = 'medicaidffs' THEN 'BCBS NC Medicaid FFS'
    when lower(insurance) = 'bcbsnc' and lower(lineOfBusiness) ='medicaid' and productDescription = 'Healthy Blue' then 'BCBS Healthy Blue Medicaid' 
    ElSE NULL
    END AS program_id
FROM `cbh-db-mirror-prod.commons_mirror.patient` 
--order by 4 nulls first
