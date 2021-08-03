{{
  config(
    materialized='table'
  )
}}


{%- set column_values = ["specialist", "other","primaryCare","surgical","behavioralHealth"] -%}

{%- set index_values = ["patientId","service_year", "service_month", "service_quarter"] -%}

with offices as (

  select a.* , 
  providerCategory 

  from {{ref('ftr_professional_services_categories')}} as a  
  left join {{ref('ftr_professional_provider_categories')}} as b  
    on a.claimId =b.claimId and a.lineId = b.lineId
  where serviceCategory ='Office Visits' 

  ),

offices_with_dates as (

  select distinct dateFrom, 
         extract(month from dateFrom) as service_month, 
         extract(year from dateFrom) as service_year, 
         extract(quarter from dateFrom) as service_quarter, 
         patientId, 
         offices.* from offices  

  join {{ref('abs_professional_flat')}} as b
    on offices.claimId = b.claimId and offices.lineId = b.lineId

),


temp as (
  select patientId, 
         service_year,
         service_quarter,
         service_month, 
	     case when providerCategory = 'unmapped' then 'other'else providerCategory end as providerCategory,  
	     count(*) as counts 
	     
	     from offices_with_dates as owd
	     group by patientId, service_year,service_quarter,service_month, providerCategory

	     ),

to_pivot as (
  select patientId, 
         service_year, 
         service_quarter, 
         service_month, 
         providerCategory, 
         counts 

  from temp 
  where patientId is not null)
	


{{pivot_table(index_values,column= "providerCategory",column_values  = column_values, value = "counts", table = "to_pivot", aggfunc="sum", suffix= 'Visits')}}






