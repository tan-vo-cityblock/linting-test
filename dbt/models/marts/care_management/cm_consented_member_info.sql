-- care management consented member info
-- author: nabig chaudhry

-- cte 1: obtaining consented and enrolled member info
with member as ( 
  select patientId,
  patientName, 
  dateOfBirth,
  lower(lineOfBusiness) as lineOfBusiness
  from  {{ ref('member') }}
  where currentState in ('consented', 'enrolled') ),

-- cte 2: obtaining member states and consent date info
consent as (
  select patientId,
  currentState,
  consentedAt
  from {{ ref('member_states') }} ),

-- cte 3: obtaining member team info
team as (
  select patientId,
  primaryCommunityHealthPartnerName
  from  {{ ref('member_primary_care_team') }} ),
  
-- cte 4: obtaining member intial acuity info
intial_acuity as (
  select patientId,
  initialMemberAcuityDescription
  from {{ ref('member_initial_acuity') }} ),
  
-- cte 5: obtaining member current acuity info
current_acuity as (
  select patientId,
  currentMemberAcuityDescription,
  targetMonthlyTendsCurrentAcuity
  from {{ ref('member_current_acuity') }} ),

-- cte 6: obtaining count of member goals completed
goals as (
  select patientId,
  count(distinct goalId) as totalGoalsCompleted
  from {{ ref('member_goals_tasks') }}
  where goalCompletedAt is not null
  group by patientId ),
  
-- cte 7: joining all together into consented member info table
final as (
  select m.patientId,
  m.patientName as fullName,
  m.dateOfBirth,
  m.lineOfBusiness,
  c.currentState,
  c.consentedAt,
  t.primaryCommunityHealthPartnerName,
  i.initialMemberAcuityDescription,
  a.currentMemberAcuityDescription,
  a.targetMonthlyTendsCurrentAcuity,
  coalesce (g.totalGoalsCompleted, 0) as totalGoalsCompleted
  from member m
  left join consent c
  on m.patientId = c.patientId
  left join team t
  on m.patientId = t.patientId
  left join intial_acuity i
  on m.patientId = i.patientId
  left join current_acuity a
  on m.patientId = a.patientId
  left join goals g
  on m.patientId = g.patientId )

-- final query
select * from final
