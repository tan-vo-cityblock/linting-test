
select * except (careTeamMemberAssignedUntil)

from {{ ref('member_care_team_all') }}

where careTeamMemberAssignedUntil is null
