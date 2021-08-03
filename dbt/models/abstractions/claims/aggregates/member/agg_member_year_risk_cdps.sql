

{{
  config(
    materialized='table'
  )
}}

with monthly as (

  select * 

  from 
  {{ref('agg_member_year_month_risk_cdps')}}
  ),


grouped as (

 select 
 monthly.partnerName, 
 monthly.lineOfBusiness,
 memberId,
 year,
 age,
 gender,
  max(demo_category)  as demo_category,
 max(cdpsAIDSH)  as cdpsAIDSH, 
 max(cdpsBABY1)  as cdpsBABY1,  max(cdpsBABY2)  as cdpsBABY2,  max(cdpsBABY3) as cdpsBABY3, max(cdpsBABY4) as cdpsBABY4, 
 max(cdpsBABY5)  as cdpsBABY5,  max(cdpsBABY6)  as cdpsBABY6,  max(cdpsBABY7) as cdpsBABY7, max(cdpsBABY8) as cdpsBABY8,
 max(cdpsCANVH)  as cdpsCANVH,  max(cdpsCANH)   as cdpsCANH,   max(cdpsCANM)  as cdpsCANM,  max(cdpsCANL)  as cdpsCANL,  
 max(cdpsCARVH)  as cdpsCARVH,  max(cdpsCARM)   as cdpsCARM,   max(cdpsCARL)  as cdpsCARL,  max(cdpsCAREL) as cdpsCAREL, 
 max(cdpsCERL)   as cdpsCERL, 
 max(cdpsCNSH)   as cdpsCNSH,   max(cdpsCNSM)   as cdpsCNSM,   max(cdpsCNSL)  as cdpsCNSL, 
 max(cdpsDDM)    as cdpsDDM,    max(cdpsDDL)    as cdpsDDL, 
 max(cdpsDIA1H)  as cdpsDIA1H,  max(cdpsDIA1M)  as cdpsDIA1M,  max(cdpsDIA2M) as cdpsDIA2M, max(cdpsDIA2L) as cdpsDIA2L, 
 max(cdpsEYEL)   as cdpsEYEL,   max(cdpsEYEVL)  as cdpsEYEVL, 
 max(cdpsGENEL)  as cdpsGENEL,
 max(cdpsGIH)    as cdpsGIH,    max(cdpsGIM)    as cdpsGIM,    max(cdpsGIL)   as cdpsGIL,
 max(cdpsHEMEH)  as cdpsHEMEH,  max(cdpsHEMVH)  as cdpsHEMVH,  max(cdpsHEMM)  as cdpsHEMM, max(cdpsHEML)   as cdpsHEML, 
 max(cdpsHIVM)   as cdpsHIVM, 
 max(cdpsHLTRNS) as cdpsHLTRNS,
 max(cdpsINFH)   as cdpsINFH,   max(cdpsINFM)   as cdpsINFM,   max(cdpsINFL)  as cdpsINFL,  
 max(cdpsMETH)   as cdpsMETH,   max(cdpsMETM)   as cdpsMETM,   max(cdpsMETVL) as cdpsMETVL,  
 max(cdpsPRGCMP) as cdpsPRGCMP, max(cdpsPRGINC) as cdpsPRGINC,
 max(cdpsPSYH)   as cdpsPSYH,   max(cdpsPSYM)   as cdpsPSYM,   max(cdpsPSYML) as cdpsPSYML, max(cdpsPSYL)  as cdpsPSYL,
 max(cdpsPULVH)  as cdpsPULVH,  max(cdpsPULH)   as cdpsPULH,   max(cdpsPULM)  as cdpsPULM,  max(cdpsPULL)  as cdpsPULL,
 max(cdpsSUBL)   as cdpsSUBL,   max(cdpsSUBVL)  as cdpsSUBVL,
 max(cdpsSKCM)   as cdpsSKCM,   max(cdpsSKCL)   as cdpsSKCL,   max(cdpsSKCVL) as cdpsSKCVL,  
 max(cdpsSKNH)   as cdpsSKNH,   max(cdpsSKNL)   as cdpsSKNL,   max(cdpsSKNVL)  as cdpsSKNVL,  
 max(cdpsRENVH)  as cdpsRENVH,  max(cdpsRENEH)  as cdpsRENEH,  max(cdpsRENM)  as cdpsRENM,  max(cdpsRENL)  as cdpsRENL,

  
from 
monthly 

group by 
monthly.partnerName, 
monthly.lineOfBusiness,
memberId, 
year,
age,
gender

),


heir as (

  {{ apply_cdps_hierarchies('grouped') }}

),


weighted as (

  {{ apply_cdps_weights(table_name='heir',
            model='CDPS+Rx',
            model_type='PROSPECTIVE',
            population='DISABLED',
            score_type='acute',
            version = 6.4
            )  
  }}

),


demo as (

  {{ apply_cdps_demo(table_name='heir',
            model='CDPS+Rx',
            model_type='PROSPECTIVE',
            population='DISABLED',
            score_type='acute',
            version='6.4'
            )
  }}

)


select * ,
ifnull(demoScore, 0)+ifnull(cdpsScore, 0)  as scoreWithDemo

from weighted 

left join 
demo 
using(partner, demo_category )

