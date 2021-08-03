WITH 

members as (
	SELECT patientId
	FROM {{ ref('member') }}
),

months as (
	SELECT patientId, eligDate, 
	CASE WHEN partnerName = 'Emblem Health' THEN 'emblem'
      ELSE lower(partnerName) END as partner -- to be replaced when partner column added to this table
	FROM {{ ref('mrt_member_month_spine_cu_denom') }}
),

costs as (
	SELECT patientId, dateFrom, amountPlanPaid, maxDateFrom
	FROM {{ ref('mrt_claims_self_service') }}
	WHERE (acsInpatientFlag OR acsEdFlag) AND claimLineStatus in ('Paid', 'Encounter')
),

periods as (
	select partner,
	-- Last 12 months of claims per partner
	min(date_sub(date_add(date_trunc(maxDateFrom, month), interval 1 month), interval 12 month)) as START_DATE_WINDOW,
	min(date_sub(date_add(date_trunc(maxDateFrom, month), interval 1 month), interval 1 day)) as END_DATE_WINDOW,
	from {{ ref('mrt_claims_self_service') }}
	group by partner
),

merged as (
	SELECT m.patientId, mm.eligDate, c.amountPlanPaid, 
	mm.eligDate between p.START_DATE_WINDOW and p.END_DATE_WINDOW as inPeriod
	FROM members m
	LEFT JOIN months mm USING(patientId)
	LEFT JOIN costs c
	ON mm.patientId = c.patientId 
	  AND mm.eligDate = date_trunc(c.dateFrom, month)
	LEFT JOIN periods p ON p.partner = mm.partner 
),

final as (
	SELECT patientId,
	sum(CASE WHEN inPeriod THEN amountPlanPaid ELSE NULL END) as impactableSpend,
	count(distinct CASE WHEN inPeriod THEN eligDate ELSE NULL END) as memberMonths,
	SAFE_DIVIDE(
		sum(CASE WHEN inPeriod THEN amountPlanPaid ELSE NULL END), 
		count(distinct CASE WHEN inPeriod THEN eligDate ELSE NULL END)
		) as impactableSpendPMPM
	FROM merged
	GROUP BY patientId
)

SELECT * FROM final