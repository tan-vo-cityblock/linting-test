{{ config(materialized = 'ephemeral') }}

with src_member as (

  select
  patientId,
  partnerName

  from {{ ref('src_member') }}

  where partnerName in

    {{ list_to_sql(dbt_utils.get_column_values(table = ref('partner_pathway_hierarchy'), column = 'partnerName')) }}

  and lower(cohortName) not like '%digital%'
  and lower(cohortName) not like '%virtual%'

),

non_disenrolled_members as (

  select
  patientId

  from {{ ref('member_states') }}
  where currentState not like 'disenrolled%'

)

select
m.patientId,
m.partnerName

from src_member m

inner join non_disenrolled_members
using (patientId)
