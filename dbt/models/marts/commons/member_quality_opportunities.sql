
{{
 config(
   materialized='ephemeral'
 )
}}

with quality_opportunities as (

  select 
    patientId,
    count(distinct measureID) as numOpenQualityOpportunities
  from {{ ref('hedis_gaps') }}
  where 
    opportunityStatus = 'OPEN' and
    isLatestCreated is true
  group by patientId

)

select * from quality_opportunities