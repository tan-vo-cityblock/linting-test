
with complex_care_mgmt_all as (

  select 
    * 
  
  from {{ ref('xpt_complex_care_mgmt_all') }}

  where complex_care_management = 'true'
    and currentState not like 'disenrolled%'
    and cohortName != 'Emblem Medicaid Digital Cohort 1'
)

select * from complex_care_mgmt_all
