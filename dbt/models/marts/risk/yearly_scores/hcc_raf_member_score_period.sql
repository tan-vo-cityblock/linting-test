
with captureCount as (

select * from {{ref('hcc_captured_count_MYFY')}}
),


captured as (

select * from {{ref('hcc_captured_current_year_MYFY')}}

),


orec as (

select * from {{ref('hcc_orec')}}

where 
eligYear = 2019
),


interactionvar as (

select * from {{ref('hcc_interaction_variable_MYFY')}}
),


demo as (

select distinct * from {{ ref('hcc_demo_MYFY') }}
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
institutionalStatus,
Original_Reason_for_Entitlement_Code_OREC,
disenrolledYear
from 
demo
),


hcc as (

select * 

from

(select distinct partner, lineofbusiness, patientID, 'DEMO' as conditiontype, RA_Factor_Type_Code as hcccode,  coefficient, 'BASESCORE' as component from demo)

union all

(select distinct partner, lineofbusiness, patientID, conditiontype,  capturedHCC, cast(capturedCoefficient as numeric) as CoeffValue, component from captured)

--not for 2019
-- union all

-- (select distinct  partner, lineofbusiness, patientID, 'INTERACTION' as conditiontype, 'HCC COUNT' as hcc, coefficient, 'ADDED' as component from captureCount)

union all

(select distinct  partner, lineofbusiness, patientID, 'INTERACTION' as conditiontype, 'OREC DISABLED' as hcc, coefficient, 'ADDED' as component from orec)

union all

(select distinct  partner,lineofbusiness, patientID, 'INTERACTION' as conditiontype, hcc, coefficient, 'ADDED' as component from interactionvar)

),


final as (

select distinct
concat(mm.partner," ",mm.lineofbusiness) as partnerLOB,
monthcount as countMonths,
esrdStatus,
hospiceStatus,
institutionalStatus,
disenrolledYear,
mm.patientID,
Original_Reason_for_Entitlement_Code_OREC,
count(distinct concat(hcc.patientID,hcccode)) as countHCCs,
sum(hcc.coefficient) as scoreHCCs

from 
mm

left join
hcc
on mm.patientID = hcc.patientID
and mm.partner = hcc.partner
and mm.lineofbusiness = hcc.lineofbusiness

group by
concat(mm.partner," ",mm.lineofbusiness),
monthcount,
esrdStatus,
hospiceStatus,
institutionalStatus,
Original_Reason_for_Entitlement_Code_OREC,
mm.patientID,
disenrolledYear
) 

select * from final
