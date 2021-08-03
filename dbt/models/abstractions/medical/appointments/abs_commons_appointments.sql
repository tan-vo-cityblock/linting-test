with ranked_commons_appointments as (

  select
    id as commonsAppointmentId,
    patientId,
    startAt as appointmentAt,
    providerName as userName,
    rank() over(partition by patientId, providerName, startAt order by createdAt desc) as rnk

  from {{ ref('src_commons_appointments') }}
),

commons_appointments as (

  select *
  from ranked_commons_appointments
  where rnk = 1
  
)

select * from commons_appointments
