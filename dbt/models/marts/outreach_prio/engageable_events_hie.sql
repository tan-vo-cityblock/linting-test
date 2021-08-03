WITH hie_rank_events AS (
  SELECT 
    *,
    ROW_NUMBER() OVER(PARTITION BY patientId ORDER BY eventAt DESC) as rankEvents
  FROM {{ ref('mrt_toc_events') }}
  WHERE visitType IN ('Inpatient', 'Emergency', 'Observation')
),

hie_latest_events AS (
	SELECT
	  patientId, 
	  patientVisitTypeEventId as eventId, 
	  patientVisitId as eventGroupId,
	  eventSource, 
	  eventType, 
	  visitType, 
	  facilityType, 
	  readmissionFlag,
	  locationName, 
	  eventAt, 
	  eventReceivedAt
	FROM hie_rank_events 
	WHERE rankEvents = 1
)

SELECT * 
FROM hie_latest_events