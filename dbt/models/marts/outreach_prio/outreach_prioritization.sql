WITH

member as (
  SELECT
    patientId,
    patientHomeMarketName as market,
    lower(lineOfBusiness) as lineOfBusiness,
    lower(cohortName) as cohortName,
    topPathwaySlug as carePathway,
    topPathwayProjectedSavingsPerMonth as medicalSavingsPMPM,
    cohortGoLiveDate,
    currentState
  FROM {{ ref('member') }}
),

------------------------------ OUTREACH CYCLE LOGIC ------------------------------
-- Determining the number of outreach cycles for a given member is difficult in
-- SQL, however, it is simpler to determine whether a given member received zero, 
-- one, or more than one outreach cycle, which is all we need for the time being.

-- 1209600 is the number of seconds in two weeks
-- 60 * 60 * 24 * 7 * 2 == 1209600
outreach_info as (
  SELECT 
    patientId, 
    attemptedAt, 
    COUNT(*) OVER ( 
      PARTITION BY patientId 
      ORDER BY UNIX_SECONDS(attemptedAt) 
      RANGE BETWEEN 1209600 PRECEDING AND CURRENT ROW
    ) AS attemptsInPreviousTwoWeeks 
  FROM {{ ref('outreach_attempt') }}
),

potential_outreach_cycles AS (
  SELECT
    patientId,
    attemptedAt,
    LEAD(attemptedAt) OVER (
      PARTITION BY patientId 
      ORDER BY attemptedAt
    ) as followedBy,
    LAG(attemptedAt) OVER (
      PARTITION BY patientId 
      ORDER BY attemptedAt
    ) as precededBy
  FROM outreach_info
  WHERE attemptsInPreviousTwoWeeks >= 3
),

overlapping_outreach_cycles AS (
  SELECT 
    patientId, 
    attemptedAt, 
    COUNT(*) OVER (
      PARTITION BY patientId 
      ORDER BY UNIX_SECONDS(attemptedAt) 
      RANGE BETWEEN 1209600 PRECEDING AND CURRENT ROW
    ) - 1 AS overlapCount
  FROM potential_outreach_cycles
  WHERE followedBy IS NULL OR precededBy IS NULL
),

-- NOTE: if outreachCyclesReceived = 2, that means there are *2 or more* outreach cycles
outreach_cycles as (
  SELECT
    patientId, 
    date_diff(current_date(), date(MAX(attemptedAt)), DAY) as daysSinceLastOutreachCycle,
    COUNT(*) AS outreachCyclesReceived
  FROM overlapping_outreach_cycles 
  WHERE overlapCount = 0 
  GROUP BY patientId 
),
------------------------------ END OUTREACH CYCLE LOGIC ------------------------------

outreach_opportunities as (
	SELECT patientId, opportunityHours, opportunityDays
	FROM {{ ref('outreach_opportunities') }}
),

impactable_spend as (
	SELECT patientId, memberMonths, impactableSpend, impactableSpendPMPM
	FROM {{ ref('impactable_spend') }}
),

engageable_events_hie as (
  SELECT patientId, eventAt as mostRecentEngageableEvent
  FROM {{ ref('engageable_events_hie') }}
),

revenue_capture_savings as (
 	SELECT 
    patientId, 
	 	sum(finaloutstandingCoeffdeduct) as coeffValue, 
	 	sum(dollarValueDeduct) as revenueCaptureSavings, 
	 	sum(dollarValueDeduct)/12 as revenueCaptureSavingsPMPM
	 FROM {{ ref('current_hcc_opportunities') }}
 	GROUP BY 1
),

care_team as (
  SELECT patientId, primaryOutreachSpecialistName, primaryLeadOutreachSpecialistName
  FROM {{ ref('member_primary_care_team') }}
),

latest_cotiviti_score as (
  SELECT
    patientId,
    predictedLikelihoodOfHospitalization as cotivitiScore,
    CASE
      WHEN predictedLikelihoodOfHospitalization <= 0.2
      THEN 'low'
      WHEN predictedLikelihoodOfHospitalization > 0.2 AND predictedLikelihoodOfHospitalization <= 0.4
      THEN 'medium'
      WHEN predictedLikelihoodOfHospitalization > 0.4
      THEN 'high'
      ELSE NULL
    END as acuity,
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY patientId ORDER BY createdAt DESC) rn 
    FROM {{ ref('mrt_cotiviti_latest_likelihood_of_hospitalization') }}
  )
  WHERE rn = 1
),

latest_outreach_attempt AS (
  SELECT patientId, latestOutreachAttemptAt,
  FROM {{ ref('outreach_attempt')}}
  GROUP BY patientId, latestOutreachAttemptAt
),

member_cte as (
  SELECT 
    * except(impactableSpendPMPM, medicalSavingsPMPM, revenueCaptureSavingsPMPM, outreachCyclesReceived),
    COALESCE(impactableSpendPMPM,0) as impactableSpendPMPM,
    CASE 
      WHEN cohortName like '%digital%' or cohortName like '%virtual%' -- cohorts not eligible for medical savings programs 
      THEN NULL
      ELSE COALESCE(medicalSavingsPMPM,0) 
    END as medicalSavingsPMPM,
    CASE 
      WHEN lineOfBusiness not in 
        ('medicare', 'dsnp', 'dual', 'duals') -- list all LOBs currently supported in revenue capture estimates 
      THEN NULL
      ELSE COALESCE(revenueCaptureSavingsPMPM,0) 
    END as revenueCaptureSavingsPMPM,
    COALESCE(outreachCyclesReceived,0) as outreachCyclesReceived,
    date_diff(current_date(), date(cohortGoLiveDate), DAY) as daysSinceCohortGoLiveDate,
    date_diff(current_date(), date(latestOutreachAttemptAt), DAY) as daysSinceLatestOutreachAttempt,
  FROM member
  LEFT JOIN outreach_cycles USING(patientId)
  LEFT JOIN outreach_opportunities USING(patientId)
  LEFT JOIN impactable_spend USING(patientId)
  LEFT JOIN revenue_capture_savings USING(patientId)
  LEFT JOIN latest_cotiviti_score USING(patientId)
  LEFT JOIN latest_outreach_attempt USING(patientId)
  LEFT JOIN care_team USING(patientId)
  LEFT JOIN engageable_events_hie USING(patientId)
),

member_tiers as (
  SELECT
    *, 
    CASE
      WHEN 
        currentState NOT IN ('attributed', 'assigned', 'contact_attempted', 'reached', 'interested', 'not_interested')
        OR primaryOutreachSpecialistName IS NOT NULL
        OR primaryLeadOutreachSpecialistName IS NOT NULL
      THEN NULL
      WHEN
        timestamp_diff(current_timestamp(), mostRecentEngageableEvent, HOUR) < 72
      THEN 1
      --------------------------------------------------------------------------
      WHEN
        market = 'North Carolina'
      THEN (
        CASE
          WHEN
            daysSinceCohortGoLiveDate > 60 
            AND currentState IN ('attributed', 'assigned', 'contact_attempted')
            AND outreachCyclesReceived > 0
          THEN 6 -- deprioritize members we can't contact after 60 days
          WHEN
            daysSinceCohortGoLiveDate <= 30
            AND outreachCyclesReceived = 0
            AND COALESCE(daysSinceLatestOutreachAttempt, 9999) > 2
          THEN 2
          WHEN
            daysSinceCohortGoLiveDate > 30
            AND outreachCyclesReceived = 0
            AND COALESCE(daysSinceLatestOutreachAttempt, 9999) > 2
          THEN 3
          WHEN
            outreachCyclesReceived = 1
            AND COALESCE(daysSinceLatestOutreachAttempt, 9999) > 2
          THEN 4
          WHEN
            outreachCyclesReceived = 2
            AND COALESCE(daysSinceLatestOutreachAttempt, 9999) > 2
          THEN 5
          ELSE NULL
        END
      )
      --------------------------------------------------------------------------
      WHEN 
        daysSinceCohortGoLiveDate > 60 AND daysSinceCohortGoLiveDate < 120
        AND currentState IN ('attributed', 'assigned', 'contact_attempted')
        AND outreachCyclesReceived = 0 
        AND COALESCE(daysSinceLatestOutreachAttempt, 9999) > 2
      THEN 2
      WHEN 
        daysSinceCohortGoLiveDate <= 60 
        AND currentState IN ('attributed', 'assigned', 'contact_attempted')
        AND outreachCyclesReceived = 0 
        AND COALESCE(daysSinceLatestOutreachAttempt, 9999) > 2
      THEN 3
      WHEN 
        daysSinceCohortGoLiveDate < 120 
        AND currentState IN ('reached', 'interested', 'not_interested')
        AND COALESCE(daysSinceLatestOutreachAttempt, 9999) > 30 
        AND acuity IN ('medium', 'high')
      THEN 4
      WHEN 
        daysSinceCohortGoLiveDate < 120 
        AND currentState IN('attributed', 'assigned', 'contact_attempted')
        AND outreachCyclesReceived = 1 
        AND COALESCE(daysSinceLatestOutreachAttempt, 9999) > 2 
        AND acuity IN ('medium', 'high')
      THEN 5
      WHEN 
        daysSinceCohortGoLiveDate > 60 AND daysSinceCohortGoLiveDate < 120 
        AND currentState IN ('attributed', 'assigned', 'contact_attempted')
        AND outreachCyclesReceived = 1 
        AND COALESCE(daysSinceLatestOutreachAttempt, 9999) > 7 
        AND acuity = 'low'
      THEN 6
      WHEN 
        daysSinceCohortGoLiveDate < 120 
        AND currentState IN ('attributed', 'assigned', 'contact_attempted')
        AND outreachCyclesReceived = 2 
        AND COALESCE(daysSinceLatestOutreachAttempt, 9999) > 2
      THEN 7
      WHEN 
        daysSinceCohortGoLiveDate < 120 
        AND currentState IN ('reached', 'interested', 'not_interested')
        AND COALESCE(daysSinceLatestOutreachAttempt, 9999) > 30 
        AND acuity = 'low'
      THEN 8
      WHEN 
        daysSinceCohortGoLiveDate >= 120 
        AND currentState IN ('attributed', 'assigned')
      THEN 9
      WHEN
        daysSinceCohortGoLiveDate >= 120
        AND currentState IN ('reached', 'interested', 'not_interested')
        AND COALESCE(daysSinceLatestOutreachAttempt, 9999) > 30
      THEN 10
      WHEN
        daysSinceCohortGoLiveDate >= 120
        AND currentState = 'contact_attempted'
        AND daysSinceLastOutreachCycle > 30
      THEN 11
      ELSE 12
    END as tier
  FROM member_cte
),

member_outreach_priority as (
  SELECT
    patientId, 
    ROW_NUMBER() OVER (
      PARTITION BY market 
      ORDER BY tier, cotivitiScore DESC
    ) as outreachCyclePriorityRankPerMarket
  FROM member_tiers
  WHERE tier IS NOT NULL
),

member_healthy_blue_responsible_party as (
  SELECT
    patient.patientId,
    lower(REGEXP_REPLACE(CONCAT(
    -- name
    COALESCE(data.Responsible_Party_Name_Prefix, ''),
    data.Responsible_Party_First_Name,
    COALESCE(data.Responsible_Party_Middle_Name, ''),
    data.Responsible_Party_Last_or_Organization_Name,
    COALESCE(data.Responsible_Party_Name_Suffix, ''),
    -- address
    data.Responsible_Person_Address_Line1,
    COALESCE(data.Responsible_Person_Address_Line2, ''),
    data.Responsible_Person_City_Name,
    data.Responsible_Person_State_Code,
    data.Responsible_Person_ZIP_Code
    ), ' ', '')) as responsibleParty
  FROM `cbh-healthyblue-data.silver_claims.member_20210629`
  WHERE patient.patientId IS NOT NULL
),

member_healthy_blue_responsible_party_rank as (
    SELECT MIN(outreachCyclePriorityRankPerMarket) as rank, responsibleParty
    FROM member_healthy_blue_responsible_party
    JOIN member_outreach_priority USING (patientId)
    GROUP BY responsibleParty
),

member_responsible_party_rank as (
  SELECT patientId, responsibleParty, rank
  FROM member_healthy_blue_responsible_party
  JOIN member_healthy_blue_responsible_party_rank USING(responsibleParty)
),

final as (
  SELECT
    mt.* except(impactableSpend, memberMonths, lineOfBusiness, cohortName),
    -- Rescale all variables into [0,1] scale
    ML.MIN_MAX_SCALER(mt.impactableSpendPMPM) OVER() AS scaledImpactableSpendPMPM,
    ML.MIN_MAX_SCALER(mt.medicalSavingsPMPM) OVER() AS scaledMedicalSavingsPMPM,
    ML.MIN_MAX_SCALER(mt.revenueCaptureSavingsPMPM) OVER() AS scaledRevenueCaptureSavingsPMPM,
    mrp.responsibleParty,
    COALESCE(mrp.rank, mop.outreachCyclePriorityRankPerMarket) as outreachCyclePriorityRankPerMarket
  FROM member_tiers mt
  LEFT JOIN member_outreach_priority mop USING(patientId)
  LEFT JOIN member_responsible_party_rank mrp USING (patientId)
)

SELECT *
FROM final
ORDER BY 
  market, 
  tier NULLS LAST,
  outreachCyclePriorityRankPerMarket NULLS LAST
