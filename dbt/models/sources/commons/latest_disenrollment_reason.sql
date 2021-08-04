--there are patients with two disenrollment reasons ("soft deletes") this sql only picks the latest one not deleted

with reason_rank as (
  select
    *,
     row_number() over (partition by patientid order by createdAt desc) as rank
  from  {{ source('commons', 'patient_disenrollment') }}
  where deletedAt is null
),

--need to get the latest reason why a patient is disenrolled
latest_reason as (
  select
    *
  from  reason_rank
  where rank = 1
)

select * from latest_reason
