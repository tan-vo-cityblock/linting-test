
with f18_selfManagedTasks as (

  select
    patientId,
    count(*) as countSelfManagedTasks
  from {{ source('commons','task') }}
  where isMemberSelfManagement is true
  group by patientId

)

select * from f18_selfManagedTasks
