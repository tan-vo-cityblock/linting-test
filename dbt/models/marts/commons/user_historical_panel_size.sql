WITH
daily_user_assignments AS (
  SELECT
    *
  FROM
    {{ ref('date_details') }} tds
  CROSS JOIN
    {{ source('commons', 'care_team') }} ct
  WHERE
    (DATE(ct.createdAt, "America/New_York") <= tds.dateDay
      AND DATE(ct.deletedAt, "America/New_York") >= tds.dateDay)
    OR (DATE(ct.createdAt, "America/New_York") <= tds.dateDay
      AND ct.deletedAt IS NULL) 
      )

  SELECT 
         dateDay,
         isFirstDayOfMonth,
         isLastDayOfMonth, 
         userId, 
         COUNT(DISTINCT u.patientId) as countMembers,
         COUNT(DISTINCT CASE WHEN ps.historicalState in ('consented', 'enrolled') THEN u.patientId ELSE NULL END) as countConsentedEnrolledMembers,
         COUNT(DISTINCT CASE WHEN ps.historicalState in ('disenrolled', 'disenrolled_after_consent') THEN u.patientId ELSE NULL END) as countDisenrolledMembers,
         COUNT(DISTINCT CASE WHEN ps.historicalState not in ('disenrolled', 'disenrolled_after_consent',  'consented', 'enrolled') THEN u.patientId ELSE NULL END) as countPreConsentedMembers,
         ARRAY_AGG(STRUCT(u.patientId, ps.historicalState) ORDER BY u.patientId) as details     
  FROM daily_user_assignments u
  LEFT JOIN {{ ref('member_historical_states') }} ps
  ON u.dateDay = ps.calendarDate AND u.patientId = ps.patientId
  WHERE dateDay <= CURRENT_DATE("America/New_York")
  GROUP BY dateDay,
         isFirstDayOfMonth,
         isLastDayOfMonth, 
         userId