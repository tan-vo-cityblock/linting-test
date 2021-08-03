
with c_map as (

  select
    patientId,
    goalLabel,
    goalTitle as goal,
    goalCompletedAt as goalCompleted,
    taskTitle as task,
    taskDueAt as taskDueDate,
    taskCompletedAt as taskCompleted

  from {{ ref('member_goals_tasks') }}

)

select * from c_map
