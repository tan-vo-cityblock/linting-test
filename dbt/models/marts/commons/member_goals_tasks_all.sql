with current_state as(
select
patientid,
currentState
from {{ ref('member_states') }}
),


goal_collaborators as (

  select
    goalGroupId,
    count(distinct userId) as numGoalCollaborators
  from {{ ref('goal_task_collaborators') }}
  group by goalGroupId

),

task_collaborators as (

  select
    taskGroupId,
    count(distinct userId) as numTaskCollaborators
  from {{ ref('goal_task_collaborators') }}
  group by taskGroupId

),

task_labels as (

  select
    taskGroupId,
    string_agg(label, ", " order by label) as taskLabels
  from {{ source('commons', 'task_label') }}
  where deletedAt is null
  group by taskGroupId

),

task as (

  select
    t.patientId,
    t.groupId as taskGroupId,
    t.id as taskId,
    trim(t.title) as taskTitle,
    t.createdById as taskCreatedById,
    t.assignedToId as taskAssignedToId,
    t.completedById as taskCompletedById,
    t.createdAt as taskCreatedAt,
    t.updatedAt as taskUpdatedAt,
    t.completedAt as taskCompletedAt,
    t.deletedAt,
    t.dueAt as taskDueAt,
    t.isMemberSelfManagement,
    t.isLegacy as taskIsLegacy,
    ifnull(t.isDraft, false) as taskIsDraft,
    t.isUrgent as isUrgentTask,
    t.status as taskStatus,
    t.description as taskDescription,
    t.goalGroupId,
    t.goalGroupId is not null as taskIsAssociatedWithGoal,
    t.templateSlug,
    case
      when t.completedAt is not null then true
      else false
    end as taskIsCompleted,
    t.batchUploadId as taskBatchUploadId,
    rank() over(partition by groupId order by deletedAt is null desc, updatedAt desc, id) as rnk
  from {{ source ('commons', 'task')}} t

),

first_due as (

  select
    replace(json_extract(body, "$.taskGroupId"), "\"", "") as taskGroupId,
    min(safe_cast(replace(json_extract(body, "$.previousValue"), "\"", "") as date)) as earliestTaskDueAt
  from {{ ref('src_commons_event') }}
  where
    type = 'taskActivityOccured' and
    replace(json_extract(body, "$.field"), "\"", "") = 'dueAt'
  group by 1

),

mapv2 as (

  select
    {{ dbt_utils.surrogate_key(['id', 't.taskGroupId']) }} as id,
    coalesce(g.patientId, t.patientId) as patientId,
    g.id as goalId,
    g.title as goalTitle,
    g.templateSlug as goalTemplateSlug,
    g.riskAreaSlug as goalRiskAreaSlug,
    g.label as goalLabel,
    g.createdAt as goalCreatedAt,
    g.updatedAt as goalUpdatedAt,
    g.completedAt as goalCompletedAt,
    g.dueAt as goalDueAt,
    g.deletedAt as goalDeletedAt,
    g.deletedAt is not null as isDeletedGoal,
    g.archivedAt,
    g.archivedAt is not null as isArchivedGoal,
    t.taskGroupId,
    tl.taskLabels,
    t.taskId,
    t.taskTitle,
    t.templateSlug as taskTemplateSlug,
    t.taskCreatedById,
    t.taskAssignedToId,
    t.taskCompletedById,
    t.taskCreatedAt,
    t.taskUpdatedAt,
    t.taskCompletedAt,
    t.taskDueAt,
    t.deletedAt as taskDeletedAt,
    t.deletedAt is not null as isDeletedTask,
    f.earliestTaskDueAt,
    t.isMemberSelfManagement,
    t.taskIsLegacy,
    g.isLegacy as goalIsLegacy,
    t.taskIsDraft,
    t.isUrgentTask,
    t.taskStatus,
    t.taskDescription,
    t.taskIsAssociatedWithGoal,
    t.taskIsCompleted,
    t.taskBatchUploadId,
    t.taskisCompleted is false AND t.taskDueAt < current_date as isOverdueTask,
    case
      when g.completedAt is not null then true
      else false
    end as goalIsCompleted
  from {{ source ('commons', 'goal')}} g
  full join task t
    on g.id = t.goalGroupId
  left join first_due f
    on trim(t.taskGroupId)  = f.taskGroupId
  left join task_labels tl
    on t.taskGroupId = tl.taskGroupId
  where
    (g.title not like '%Administrative Tasks%' or g.title is null) and
    (t.rnk = 1 or t.rnk is null)

),

final as (

  select
    mapv2.*,
    numGoalCollaborators,
    numTaskCollaborators,
    case
        when lower(currentState) not like '%disenrolled%'
            and goalCompletedAt is null
            and isDeletedGoal is false
            and isDeletedTask is false
            and taskIsLegacy is false
            and taskIsCompleted is false
            and taskIsDraft is false
            and isMemberSelfManagement is false
            then true
        else false end as isVisibleTaskInCommonsInbox
  from mapv2
  left join goal_collaborators gc
    on mapv2.goalId = gc.goalGroupId
  left join task_collaborators
    using ( taskGroupId )
  left join current_state using (patientid)
)

select *
from final
