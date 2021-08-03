
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
institutionalStatus,
Original_Reason_for_Entitlement_Code_OREC,
disenrolledYear
from 
demo
),


hcc as (

select * 

from

(select distinct eligyear as year, partner, lineofbusiness, patientID, 'DEMO' as conditiontype, RA_Factor_Type_Code as hcccode,  coefficient, 'BASESCORE' as component from demo)

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

select distinct
mm.year,
concat(mm.partner," ",mm.lineofbusiness) as partnerLOB,
monthcount as countMonths,
esrdStatus,
hospiceStatus,
institutionalStatus,
disenrolledYear,
mm.patientID,
Original_Reason_for_Entitlement_Code_OREC,
countHCCs,
round(scoreHCCs, 3) as scoreHCCs

from 
mm

left join
(select distinct
year,
patientID,
partner,
count(distinct concat(hcc.patientID,hcccode)) as countHCCs
from
hcc
group by
year,
patientID,
partner)  hccCt
on mm.patientID = hccCt.patientID
and mm.partner = hccCt.partner
and mm.year = hccCt.year

left join
(select distinct
year,
patientID,
partner,
sum(coefficient) as scoreHCCs
from
(select distinct
year,
patientID,
partner,
hcccode,
coefficient
from hcc)
group by
year,
patientID,
partner ) hccSUm
on mm.patientID = hccSUm.patientID
and mm.partner = hccSUm.partner
and mm.year = hccSUm.year
) 

select * from final where year is not null
