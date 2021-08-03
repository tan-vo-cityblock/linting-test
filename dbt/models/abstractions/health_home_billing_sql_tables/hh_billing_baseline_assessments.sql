with comp as (
SELECT
patientId,
date(minComprehensiveAssessmentAt) as minComprehensiveAssessmentdate,
date(maxComprehensiveAssessmentAt) as maxComprehensiveAssessmentdate,
--want the most updated date
case when date(minComprehensiveAssessmentAt) <> date(maxComprehensiveAssessmentAt) then maxComprehensiveAssessmentAt
    else minComprehensiveAssessmentAt end as completedat_comp
FROM  {{ ref('member_commons_completion') }} mcc
),

initial_and_comp as (SELECT
patientId,
date(completedAt) as baseline_completedAt,
minComprehensiveAssessmentdate,
maxComprehensiveAssessmentdate,
--want to make sure we do not have double dates for these assessments
case when date(completedat_comp) <> date(completedAt) then completedat_comp
else completedAt end as completedAt,
FROM {{ source('commons', 'baseline_assessment_completion') }} bac
left join comp using (patientid)
),

hh_baseline_final as (
select
patientid,
completedAt,
case when completedAt is not null then 1 else 5 end as completed_assess,
date(completedAt) as assess_date,
case when date(completedAt) > baseline_completedAt
    OR date(completedAt) > minComprehensiveAssessmentdate
   then 5 else 1 end as initial_assessment,
from initial_and_comp
inner join {{ ref('hh_billing_reporting_dates') }}
    on ((date(completedAt) <= reporting_date
        and extract (year from completedAt)||'.'||extract (quarter from completedAt) = reporting_quarter_yr)
        OR completedAt is null)
)

select * from hh_baseline_final
