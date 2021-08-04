
with captureCount as (

select * from {{ref('hcc_captured_count')}}
),

orec as (

select * from {{ref('hcc_orec')}}
),


interactionvar as (

select * from {{ref('hcc_interaction_variable')}}
),


captured as (

select * from {{ref('hcc_captured_current_year')}}
),


demo as (

select distinct * from {{ ref('hcc_demo') }}
),


mm as (

select distinct
eligYear as year,
patientID, 
partner, 
lineofbusiness, 
monthcount,
esrdStatus,
hospiceStatus,
institutionalStatus
from 
demo
),


Year_payer_mm as (
select distinct
year,
partner, 
lineofbusiness, 
ifnull(sum(monthcount),0) as payer_month_count

from
(select distinct
eligYear as year,
patientID, 
partner, 
lineofbusiness, 
monthcount

from 
demo)

group by
year,
partner, 
lineofbusiness
),


hcc as (

select * 
from

(select distinct eligyear as year, partner, lineofbusiness, patientID, 'DEMO' as conditiontype, RA_Factor_Type_Code as hcccode, coefficient, 'BASESCORE' as component from demo)

union all

(select distinct clmyear, partner, lineofbusiness, patientID, conditiontype,  capturedHCC, cast(capturedCoefficient as numeric) as CoeffValue, component from captured)

union all

(select distinct clmyear, partner, lineofbusiness, patientID, 'INTERACTION' as conditiontype, 'HCC COUNT' as hcc, coefficient, 'ADDED' as component from captureCount)

union all

(select distinct eligyear, partner, lineofbusiness, patientID, 'INTERACTION' as conditiontype, 'OREC DISABLED' as hcc, coefficient, 'ADDED' as component from orec)

union all

(select distinct clmyear, partner,lineofbusiness, patientID, 'INTERACTION' as conditiontype, hcc, coefficient, 'ADDED' as component from interactionvar)

),


final as (

select *,
case when esrdStatus = true then ((hcc_score / 1.059) * (1-0.0509) ) 
     else ((hcc_score / 1.069) * (1-0.0509) ) 
     end as codingIntensityFactor

from
(select distinct
year,
partner, 
lineofbusiness,
monthcount,
esrdStatus,
hospiceStatus,
institutionalStatus,
component, 
conditiontype, 
concat(component,' ',conditiontype) as combinedTitle,
patientID,
count( distinct concat(patientID,hcccode)) as countHCC,
sum(coefficient) as hcc_score

from 
hcc

left join
mm
using
(patientID, partner, lineofbusiness, year)


left join
Year_payer_mm
using
(partner, lineofbusiness, year)

group by
year,
partner, 
lineofbusiness,
monthcount,
esrdStatus,
hospiceStatus,
institutionalStatus,
component, 
conditiontype, 
concat(component,' ',conditiontype),
patientID
)
) 

select * from final
