WITH hedis_gaps AS (
SELECT
  externalId AS patientId,
  rateId, 
  inverse, 
  performanceYear,
  episodeStartDate,
  measureID,
  CASE
    WHEN b.name is null 
    then 'No measure name' 
    else b.name
  END AS measureIdName,
  status AS qualityOpportunity,
  CASE
    WHEN numeratorPerformanceMet = 0 and denominatorExclusion = 1 THEN "EXCLUSION"
    WHEN numeratorPerformanceMet = 1 AND inverse = 0 THEN "CLOSED"
    WHEN numeratorPerformanceMet = 1 AND inverse = 1 THEN "OPEN"
    WHEN numeratorPerformanceMet = 0 AND inverse = 0 THEN "OPEN"  
    WHEN numeratorPerformanceMet = 0 AND inverse = 1 THEN "CLOSED" 
  ELSE
  "No quality gaps"
END
  AS opportunityStatus,
  createdAt
FROM 
 {{ source('able', 'measure_results') }}  a

left JOIN
 {{ source('hedis_codesets', 'able_measure_name_ref') }} b
 on a.measureID = b.id
 and cast(a.performanceYear as string) = b.year
 
 Where lower(program) not like '%testing%'
),


--technically I could've taken a simple max here, however I'm future-proofing a little bit in case we no longer get all measures every day.

max_created_at AS (
SELECT patientId, measureId, max(createdAt) as maxCreatedAt FROM hedis_gaps
GROUP BY patientId, measureId
)

SELECT hg.*,
       CASE WHEN createdAt = maxCreatedAt 
            THEN TRUE 
            ELSE FALSE
        END AS isLatestCreated
FROM hedis_gaps  hg
     INNER JOIN max_created_at mca
     USING (patientId, measureId)