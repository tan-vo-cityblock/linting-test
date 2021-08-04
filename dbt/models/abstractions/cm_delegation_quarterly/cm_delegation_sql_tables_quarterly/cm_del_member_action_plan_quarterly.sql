-- Member action plan filter, all goals and tasks

with map AS (
  SELECT
  g.patientId,
  g.id as goalId,
  g.title as goalTitle,
  g.createdAt as goalCreatedAt,
  case when extract (year from g.deletedAt)||'.'||extract (quarter from g.deletedAt)  = reporting_quarter_yr then g.deletedAt else null end as goalDeletedAt,
  case when extract (year from g.completedAt)||'.'||extract (quarter from g.completedAt)  = reporting_quarter_yr then g.completedAt else null end as goalCompletedAt,
  case when extract (year from g.dueAt)||'.'||extract (quarter from g.dueAt)  = reporting_quarter_yr then g.dueAt else null end as goalDueAt,
  t.groupId as taskGroupId,
  t.id as taskId,
  t.title as taskTitle,
  t.createdAt as taskCreatedAt,
  case when  extract (year from t.deletedAt)||'.'||extract (quarter from t.deletedAt)  = reporting_quarter_yr then t.deletedAt else null end as taskDeletedAt,
  case when extract (year from t.completedAt)||'.'||extract (quarter from t.completedAt)  = reporting_quarter_yr then t.completedAt else null end as taskCompletedAt,
  case when extract (year from t.dueAt)||'.'||extract (quarter from t.dueAt)  = reporting_quarter_yr then t.dueAt else null end as taskDueAt,
  t.isMemberSelfManagement,
  g.isLegacy as goalIsLegacy,
  t.isLegacy as taskIsLegacy
  FROM {{ source('commons', 'goal') }} g
  LEFT JOIN {{ source('commons', 'task') }} t
  ON g.id = t.goalGroupId AND t.deletedAt IS NULL
--we want to make sure createdAt dates are between delegation date AND reporting date.
--Other dates act the same but if they are after the reporting date, they are null
  INNER join {{ ref('cm_del_reporting_dates_quarterly') }}
    on extract (year from g.createdAt)||'.'||extract (quarter from g.createdAt)  = reporting_quarter_yr
    or  extract (year from t.createdAt)||'.'||extract (quarter from t.createdAt)  = reporting_quarter_yr
  INNER join {{ ref('cm_del_delegation_dates_ytd') }} dd
     on  (date(g.createdAt) >= delegation_at
         or  date(t.createdAt) >= delegation_at)
     and g.patientId = dd.patientid
  WHERE g.deletedAt IS NULL AND
    g.title NOT LIKE '%Administrative Tasks%' -- exclude legacy admin tasks

),

dates as(
    select
        patientid,
        goalId,
        taskId,
        min(taskCreatedAt) over () AS generated_care_plan_at,
        max(goalCreatedAt) over() AS max_goal_created,
        max(goalCompletedAt) over() AS max_goal_completed,
        max(taskCreatedAt) over () AS max_task_created,
        max(taskCompletedAt) over() AS max_task_completed
    FROM map
),

updated_care_plan_at as (
    select
        patientid,
        goalId,
        taskId,
        generated_care_plan_at,
        GREATEST(max_goal_created, max_goal_completed, max_task_created, max_task_completed) as updated_care_plan_at
    from dates

),

updated_care_plan_at_2 as (
    select
        patientid,
        goalId,
        taskId,
        case
            when date(generated_care_plan_at) != date(updated_care_plan_at.updated_care_plan_at) then updated_care_plan_at
            else null end as updated_care_plan_at_2
    from updated_care_plan_at
    where generated_care_plan_at is not null
),

-- To be changed if we decide not to use this for goal due dates pre-feature release:
max_task_due as (
  SELECT
  patientid,
  goalId,
  max(taskDueAt) as goalMaxTaskDueAt
  FROM map

  WHERE goalDueAt is NULL
  GROUP BY 1,2

),

map_final as (SELECT
  map.*,
  CASE
    WHEN DATE_DIFF(COALESCE(goalDueAt, goalMaxTaskDueAt), CAST(goalCreatedAt as date), day) <= 90 then "short_term_goal"
    WHEN DATE_DIFF(COALESCE(goalDueAt, goalMaxTaskDueAt), CAST(goalCreatedAt as date), day) > 90 then "long_term_goal"
  ELSE NULL END as goalType,
  generated_care_plan_at,
  updated_care_plan_at_2
FROM map
LEFT JOIN updated_care_plan_at_2 using (patientid,goalId,taskId)
LEFT JOIN dates using (patientid,goalId,taskId)
LEFT JOIN max_task_due mt using(patientid)
),

--need flags for goals -completed/closed, created, short, long
Goal_flags as (
    Select
        map.Patientid,
        map.goalid,
        map.taskid,
        coalesce(sum(case when map.generated_care_plan_at is not null then 1 else 0 end) > 0,false) as generated_care_plan,
        coalesce(sum(case when map.updated_care_plan_at_2 is not null then 1 else 0 end) > 0,false) as updated_care_plan,
        coalesce(
            SUM(case when (map.goalcreatedat is not null and map.goaldeletedat is null
                and map.goaltype = 'short_term_goal') then 1
                else 0 end
               ) > 0,false
        ) as short_term_goal,
        coalesce(
            SUM(case when (map.goalcreatedat is not null and map.goaldeletedat is null
                and map.goaltype = 'long_term_goal') then 1
                else 0 end
               ) > 0,false
        ) as long_term_goal,
        coalesce(
            SUM(case when (map.goalcreatedat is not null and map.goaldeletedat is null
                and map.goalcompletedat is not null
                and map.goaltype = 'short_term_goal') then 1
                else 0 end
               ) > 0,false
        ) as short_term_goal_closed,
        coalesce(
            SUM(case when (map.goalcreatedat is not null
                and map.goaldeletedat is null
                and map.goalcompletedat is not null
                and map.goaltype = 'long_term_goal') then 1
                else 0 end
               ) > 0,false
        ) as long_term_goal_closed
    From map_final map
    group by 1,2,3
)

select
map_final.*,
goal_flags.generated_care_plan,
goal_flags.updated_care_plan,
Goal_flags.short_term_goal,
Goal_flags.long_term_goal,
Goal_flags.short_term_goal_closed,
Goal_flags.long_term_goal_closed,
Goal_flags.short_term_goal or Goal_flags.long_term_goal as any_goal_created,
Goal_flags.short_term_goal_closed or Goal_flags.long_term_goal_closed as any_goal_closed,
CASE
    when (Goal_flags.short_term_goal_closed or Goal_flags.long_term_goal_closed) = true
    then goalcompletedat else null end as any_goal_closed_at
from map_final
left join Goal_flags using (patientId,goalid,taskid)
