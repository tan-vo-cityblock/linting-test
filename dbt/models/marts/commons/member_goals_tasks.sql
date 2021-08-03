select *
from {{ ref('member_goals_tasks_all') }}
where
  goalDeletedAt is null and
  taskDeletedAt is null
