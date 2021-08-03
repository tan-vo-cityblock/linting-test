
{{
 config(
   materialized='ephemeral'
 )
}}

with legacy_priority as (

	select patientId
	from {{ source('member_programs', 'src_outreach_prioritization') }}
	where isPriority is true
	group by patientId

)

select * from legacy_priority
