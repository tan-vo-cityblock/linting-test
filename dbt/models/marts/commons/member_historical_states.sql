WITH

  disenrolled_after_consent_patient_state as (
    SELECT ps.* except(currentState),
    case when ps.currentState = 'disenrolled' 
          and ps.createdAt > ms.consentedAt
         then 'disenrolled_after_consent'
      else ps.currentState
    end as currentState 
    
    from {{ source('commons', 'patient_state') }} ps
    left join  
    {{ ref('member_states') }} ms USING (patientId)
  ),

  date_spine AS (
  SELECT
    calendarDate
  FROM
    UNNEST( GENERATE_DATE_ARRAY(DATE('2018-06-01'), CURRENT_DATE(), INTERVAL 1 DAY) ) AS calendarDate 
    ),
  
  first_last_date_flags AS (
  SELECT calendarDate,
          DATE_TRUNC(calendarDate, MONTH) AS firstDateOfMonth,
          DATE_SUB(DATE_TRUNC(DATE_ADD(calendarDate, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) AS lastDateOfMonth

        FROM date_spine

  ),

  if_first_last_date_flags AS
  ( SELECT calendarDate,
            firstDateOfMonth,
            lastDateOfMonth,

            CASE WHEN calendarDate=firstDateOfMonth
            THEN TRUE
            ELSE FALSE
            END AS isFirstDateOfMonth,

            CASE WHEN calendarDate=lastDateOfMonth
            THEN TRUE
            ELSE FALSE
            END AS isLastDateOfMonth
            FROM first_last_date_flags


  ),
    
  timestamped_date_spine AS (
  SELECT
    calendarDate,
    isLastDateOfMonth,
    isFirstDateOfMonth,
    --setting calendar timestamp to EST 23:59:59 using 100799 seconds to capture all times prior with a join, this means everything prior to midnight EST counts that day.
    TIMESTAMP_ADD(TIMESTAMP(calendarDate), INTERVAL 100799 SECOND) AS calendarDateTimestamp
  FROM
    if_first_last_date_flags 
    ),

  historical_patient_state AS (
  SELECT
    *
  FROM
    timestamped_date_spine tds
  CROSS JOIN
    disenrolled_after_consent_patient_state dps
  WHERE
    (dps.createdAt < tds.calendarDateTimestamp
      AND dps.deletedAt > tds.calendarDateTimestamp)
    OR (dps.createdAt < tds.calendarDateTimestamp
      AND dps.deletedAt IS NULL) 
      )
  
  SELECT 
         hps.id AS patientStateId,
         hps.calendarDate, 
         hps.calendarDateTimestamp,
         hps.isLastDateOfMonth,
         hps.isFirstDateOfMonth, 
         hps.patientId, 
         hps.currentState AS historicalState,
         hps.updatedById,
         hps.createdAt AS patientStateCreatedAt,
         hps.deletedAt AS patientStateDeletedAt,
         date_diff(hps.calendarDate, m.cohortGoLiveDate, day) AS calendarDaysSinceCohortGoLive
  
FROM historical_patient_state hps
INNER JOIN {{ ref('src_member') }} m USING (patientId)