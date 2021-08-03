WITH conversation_direction_count as (
select
patientId,
conversationId,
min(providerCreatedAt) as startdate,
--this count is to see the conversations with both an outgoing and an incoming message which will be considered complete
count(distinct direction) as count_of_directions
FROM {{ ref('sms_session_conversation') }}
WHERE patientId is not null
group by 1,2
),

--this limits the texts to just outgoing messages
sms_sessions as (
select distinct
patientId,
conversationId
from {{ ref('sms_session_conversation') }}
where direction = "outgoing"
and patientId is not null
),

interventions_other_final as (
select
patientid,
date(startdate) as intervention_date,
extract (year from date(startdate))||'.'||extract (quarter from date(startdate)) as completed_quarter_yr,
case when count_of_directions > 1 then 1
    else 5 end as completed_intervention,
5 as mode,
1 as target,
5 as outreach,
1 as care_coord_health_promote,
5 as transition_care,
5 as patient_family_support,
5 as comm_social,
5 as care_manage,
from sms_sessions
left join conversation_direction_count using (patientId,conversationid)
),

final_intervations_progressnotes as (
select * from interventions_other_final
union all
select * from {{ ref('hh_billing_progress_notes') }}
)

select
patientId,
intervention_date,
completed_quarter_yr,
mode,
target,
completed_intervention,
outreach,
care_manage,
care_coord_health_promote,
transition_care,
patient_family_support,
comm_social
from final_intervations_progressnotes
inner join {{ ref('hh_billing_reporting_dates') }}
    on date(intervention_date) <= reporting_date
     and completed_quarter_yr = reporting_quarter_yr
