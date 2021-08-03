
{{ config(materialized='ephemeral') }}

with xwalk as (
  select distinct 
      memberId as patientId, 
      externalId
  from {{ source('member_index', 'member_datasource_identifier') }}
  where datasourceId in (9,19,20,21)
)

select 
  x.patientId, 
  max(m.dateEffective.from) as tuftsEnrollmentDate
from {{ source('tufts', 'Member_*') }} m
inner join xwalk x on m.memberIdentifier.partnerMemberId = x.externalId
where m.dateEffective.from >="2019-11-01"
group by x.patientId
