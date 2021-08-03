WITH initial_acuity AS (
SELECT 
  row.patientId, 
  row.updatedByUserId, 
  row.score AS initialMemberAcuityScore, 
  row.memberAcuityDescription AS initialMemberAcuityDescription, 
  row.memberAcuityDateEST AS initialMemberAcuityDateEST,
  --calculates the number of months since initial acuity was first assigned
  DATE_DIFF(current_date("America/New_York"), row.memberAcuityDateEST, month) AS monthsSinceInitialMemberAcuity,
  --extracts first date of month so we can group members together for analysis if neccessary
  DATE_TRUNC(row.memberAcuityDateEST, month) AS initialMemberAcuityCreatedAtMonth,
  row.memberAcuityCreatedAt AS initialMemberAcuityCreatedAt,
  row.memberAcuityDeletedAt AS initialMemberAcuityDeletedAt
--takes first row from our historical acuity table
FROM (
  SELECT ARRAY_AGG(t ORDER BY memberAcuityCreatedAt ASC LIMIT 1 )[OFFSET(0)] row 
  FROM {{ ref('member_historical_acuity') }} t
  GROUP BY patientId 
) 
),

unique_acuity AS (
    SELECT patientId, 
    count(DISTINCT score) AS unique_acuities 
    FROM {{ ref('member_historical_acuity') }}
    GROUP BY patientId
),

acuity_change AS (
    SELECT patientId, 
    CASE WHEN unique_acuities > 1 
        THEN TRUE 
        ELSE FALSE 
        END AS hasEverChangedAcuity
    FROM unique_acuity
)

SELECT * 
FROM initial_acuity 
LEFT JOIN acuity_change 
USING(patientId)



