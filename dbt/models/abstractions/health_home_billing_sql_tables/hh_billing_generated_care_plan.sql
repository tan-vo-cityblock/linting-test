SELECT
patientId,
minTaskAt,
case when minTaskAt is not null then 1 else 5 end as completed_plan,
date(minTaskAt) as plan_date,
1 as initial_plan
FROM {{ ref('member_commons_completion') }}  mcc
inner join {{ ref('hh_billing_reporting_dates') }}
    on ((date(minTaskAt) <= reporting_date
        AND extract (year from minTaskAt)||'.'||extract (quarter from minTaskAt) = reporting_quarter_yr)
        OR minTaskAt is null)
