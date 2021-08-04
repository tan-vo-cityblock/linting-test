WITH 

members as (
  SELECT *
  FROM {{ ref('member') }}
),

outreach_attempts as (
  SELECT *
  FROM {{ ref('outreach_attempt') }}
),

outreach_times as (
  SELECT id, patientId, direction, modality, 
  EXTRACT(HOUR from attemptedAt AT TIME ZONE 'America/New_York') as attemptedAtHour, -- Change to case statement by market timezone
  EXTRACT(DAYOFWEEK FROM attemptedAt) as attemptedAtDay
  FROM outreach_attempts
  WHERE direction = 'outbound' -- Since we should also surface hours where inbound calls were made but no outbound
  AND NOT (personReached = 'member' AND outcome <> 'hungUp') -- Since if there were times where member was responsive previously, should preserve them
),

hours as (
  SELECT patientId, GENERATE_ARRAY(0, 23) as hoursOfDay
  FROM members
),

hours_unnested as (
  SELECT patientId, hourOfDay,
  case
    when hourOfDay < 8 OR hourOfDay > 18 then 'outsideWorkingHours'
    when hourOfDay = 8 THEN '8-9a'
    when hourOfDay = 9 THEN '9-10a'
    when hourOfDay = 10 THEN '10-11a'
    when hourOfDay = 11 THEN '11a-12p'
    when hourOfDay = 12 THEN '12-1p'
    when hourOfDay = 13 THEN '1-2p'
    when hourOfDay = 14 THEN '2-3p'
    when hourOfDay = 15 THEN '3-4p'
    when hourOfDay = 16 THEN '4-5p'
    when hourOfDay = 17 THEN '5-6p'
    when hourOfDay = 18 THEN '6-7p'
    else null end as hourOfDayName
  FROM hours, UNNEST(hoursOfDay) as hourOfDay
),

days as (
  SELECT patientId, GENERATE_ARRAY(1,7) as daysOfWeek
  FROM members
),

days_unnested as (
  SELECT patientId, dayOfWeek, 
  case 
    when dayOfWeek = 1 THEN 'Sun'
    when dayOfWeek = 2 THEN 'Mon'
    when dayOfWeek = 3 THEN 'Tue'
    when dayOfWeek = 4 THEN 'Wed'
    when dayOfWeek = 5 THEN 'Thu'
    when dayOfWeek = 6 THEN 'Fri'
    when dayOfWeek = 7 THEN 'Sat'
    else null end as dayOfWeekName
  FROM days, UNNEST(daysOfWeek) as dayOfWeek
),

outreach_attempts_with_timing as (
  SELECT h.patientId, h.hourOfDay, h.hourOfDayName, d.dayOfWeek, d.dayOfWeekName, ot.* except(patientId)
  FROM hours_unnested h
  LEFT JOIN days_unnested d
  ON h.patientId = d.patientId
  LEFT JOIN outreach_times ot
  ON ot.patientId = h.patientId and ot.attemptedAtHour = h.hourOfDay AND ot.attemptedAtDay = d.dayOfWeek
),

hours_missing as (
  SELECT patientId, hourOfDayName as hourOfDay, COUNT(DISTINCT id) as countAttempts
  FROM outreach_attempts_with_timing
  WHERE hourOfDayName != 'outsideWorkingHours'
  GROUP BY 1, 2
  HAVING COUNT(DISTINCT id) = 0
),

days_missing as (
  SELECT patientId, dayOfWeekName as dayOfWeek, COUNT(DISTINCT id) as countAttempts
  FROM outreach_attempts_with_timing
  WHERE dayOfWeekName != 'Sun'
  GROUP BY 1, 2
  HAVING COUNT(DISTINCT id) = 0
),

agg_hours_missing as (
  SELECT m.patientId, 
  array_to_string(array_agg(cast(h.hourOfDay as string)), '; ') as opportunityHours,
  FROM members m
  LEFT JOIN hours_missing h
  ON m.patientId = h.patientId
  group by m.patientId
),

agg_days_missing as (
  SELECT m.patientId, 
  array_to_string(array_agg(cast(d.dayOfWeek as string)), '; ') as opportunityDays,
  FROM members m
  LEFT JOIN days_missing d
  ON m.patientId = d.patientId
  group by m.patientId
),

final as (
  SELECT m.patientId, 
  opportunityHours,
  opportunityDays
  FROM members m
  LEFT JOIN agg_hours_missing h
  ON m.patientId = h.patientId
  LEFT JOIN agg_days_missing d 
  ON m.patientId = d.patientId
)

SELECT * FROM final
