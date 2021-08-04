with info as (
  select patientId, 
  patientName
  from `cityblock-analytics.mrt_commons.member` ),
  
map as (
  select patientId,
  goalTitle,
  taskTitle,
  taskDueAt,
  goalCreatedAt,
  goalCompletedAt,
  case
    when goalCompletedAt is null then 'In Progress'
    when goalCompletedAt is not null then 'Completed'
    else string(goalCompletedAt)
    end as goalCompletion,
  taskTitle as mapIntervention,
   case
    when taskCompletedAt is null then 'In Progress'
    when  taskCompletedAt is not null then 'Completed'
    else string(taskCompletedAt)
    end as interventionCompletion
  from `cityblock-analytics.mrt_commons.member_goals_tasks` ),
  
final as (
  select i.patientId as memberId,
  i.patientName as memberName,
  date(m.goalCreatedAt) as goalCreationDate,
  date(goalCompletedAt) as goalCompletedDate,
  m.goalTitle as mapGoal,
  m.taskDueAt as dueDate,
  m.taskTitle as mapIntervention,
  m.interventionCompletion,
  m.goalCompletion as goalCompletionStatus

  from info i
  inner join map m
  on i.patientId = m.patientId )
  
select * from final
order by mapGoal desc nulls last
-- putting the nulls at the end.
