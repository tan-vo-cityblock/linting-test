{{ config(tags = ["evening"]) }}


--start with reminder history
with clin_reminders as (

select distinct
*
from
{{ref('mrt_elation_clinical_reminder_int')}}
),


maxdates as (

select distinct
max(runDate) as maxDate

from
clin_reminders
),


final as (

select distinct
clin_reminders.date,
clin_reminders.patient_id,
clin_reminders.patient_system,
clin_reminders.first_name,
clin_reminders.last_name,
clin_reminders.date_of_birth,
clin_reminders.gender,
clin_reminders.measure_code as measure_id,
clin_reminders.measure_status,
clin_reminders.provider_assigned_npi,
clin_reminders.provider_assigned,
clin_reminders.practice_name,
case when clin_reminders.measure_status = 'CLOSED' then cast(null as string)
     else clin_reminders.evidence
          end as additional_context
from
clin_reminders

inner join
maxdates
on extract(date from clin_reminders.rundate)  = extract(date from maxdates.maxdate)
)


select * from final