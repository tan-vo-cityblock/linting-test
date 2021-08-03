with outreach_attempts as (

  select * except (
    groupedModality, isScheduledOutreach, outreachOutcome,
    outreachNotInterestedReason, interactionAt, attendees, noteText, createdAt
  ),

    isScheduledOutreach as isScheduledVisit,
    outreachOutcome as outcome,
    interactionAt as attemptedAt,
    outreachNotInterestedReason as notInterestedReason,
    attendees as personReached,
    noteText as notes,
    createdAt as notesCreatedAt

  from {{ ref('abs_commons_notes') }}
  where
    noteType = 'outreach' and
    deletedAt is null

),

latest_outreach_attempt AS (
          SELECT patientId, max(attemptedAt) AS latestOutreachAttemptAt, min(attemptedAt) AS earliestOutreachAttemptAt 
          FROM outreach_attempts
          GROUP BY patientId
          ),

latest_successful_outreach_attempt AS (
          SELECT patientId, max(attemptedAt) AS latestSuccessfulOutreachAttemptAt
          FROM outreach_attempts
          WHERE personReached = 'member'
          GROUP BY patientId
          )

SELECT oa.*,
    u.userName AS outreachAttemptUserName,
    u.userRole AS outreachAttemptUserRole, 
    u.userEmail AS outreachAttemptUserEmail,
    loa.latestOutreachAttemptAt,
    loa.earliestOutreachAttemptAt,
    lsoa.latestSuccessfulOutreachAttemptAt,
    CASE WHEN ROW_NUMBER() OVER(PARTITION BY oa.patientId ORDER BY oa.attemptedAt DESC, oa.notesCreatedAt DESC) = 1
        THEN TRUE 
        ELSE FALSE 
        END AS isLatestOutreachAttempt,
    CASE WHEN ROW_NUMBER() OVER(PARTITION BY oa.patientId ORDER BY oa.attemptedAt, oa.notesCreatedAt) = 1
        THEN TRUE 
        ELSE FALSE 
        END AS isEarliestOutreachAttempt
FROM outreach_attempts oa
LEFT JOIN {{ ref('user') }} u
using (userId)
LEFT JOIN latest_outreach_attempt loa
ON loa.patientId=oa.patientId
LEFT JOIN latest_successful_outreach_attempt lsoa
ON lsoa.patientId=oa.patientId
