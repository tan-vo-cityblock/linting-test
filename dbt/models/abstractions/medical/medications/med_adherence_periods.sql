with claims as (

  select distinct
    memberIdentifier as patientId,
    ndc, 
    dateFilled,
    fillEnd
  
  from {{ ref('abs_medications') }}

  where 
    memberIdentifierField = 'patientId' and 
    daysSupply > 0
  
),

overlap AS (
  SELECT DISTINCT
    c.patientId,
    mld.description AS drugClass,
    c.dateFilled,
    c.fillEnd,
    CASE
      WHEN LAG(c.fillEnd) OVER(PARTITION BY c.patientId, mld.description ORDER BY c.dateFilled, c.fillEnd ASC) >= c.dateFilled 
      THEN 1 ELSE 0 END AS overlapFlag
  FROM claims c
  INNER JOIN {{ source('hedis_codesets', 'mld_med_list_to_ndc') }} mld
  ON c.ndc = mld.ndc_code),
    
raw_periods AS (
  SELECT * EXCEPT (overlapFlag),
    ROW_NUMBER() OVER(PARTITION BY patientId, drugClass ORDER BY dateFilled, fillEnd) - 
      SUM(overlapFlag) OVER(PARTITION BY patientId, drugClass ORDER BY dateFilled, fillEnd) AS memberPeriodId
  FROM overlap)

SELECT patientId, drugClass, memberPeriodId, MIN(dateFilled) AS dateFilled, MAX(fillEnd) AS fillEnd
FROM raw_periods
GROUP BY patientId, drugClass, memberPeriodId
ORDER BY patientId, drugClass, memberPeriodId