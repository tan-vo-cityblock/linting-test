
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

select *

from
{{ref('hcc_captured_current_year')}}
),


demo as (

select distinct *

from
{{ ref('hcc_demo') }}
),


mm as (

select distinct
eligyear as year,
patientID, 
partner, 
age,
gender,
lineofbusiness, 
monthcount,
esrdStatus,
hospiceStatus,
institutionalStatus,
newEnrollee,
Original_Reason_for_Entitlement_Code_OREC ,
disenrolledYear

from 
demo
),


hcc as (

select * 

from

(select distinct eligyear as year, partner, lineofbusiness, patientID, 'DEMO' as conditiontype, RA_Factor_Type_Code as hcccode, 'Demographic component of HCC score' as HCCDescription, coefficient, 'BASESCORE' as component ,'mmr' as source, cast(null as date) as firstCodedDate, cast(null as date) as paiddate from demo)

union all

(select distinct clmyear, partner, lineofbusiness, patientID, conditiontype,  capturedHCC, HCCDescription, cast(capturedCoefficient as numeric) as CoeffValue, component, source,firstCodedDate, paiddate from captured)

union all

(select distinct clmyear, partner, lineofbusiness, patientID, 'INTERACTION' as conditiontype, 'HCC COUNT' as hcc, concat('Added ',capCount,' HCCs') as HCCDescription, coefficient, 'ADDED' as component, 'interaction' as source, cast(null as date) as firstCodedDate, cast(null as date) as paiddate from captureCount)

union all

(select distinct eligyear, partner, lineofbusiness, patientID, 'INTERACTION' as conditiontype, 'OREC DISABLED' as hcc, 'Originally disabled flag' as HCCDescription, coefficient, 'ADDED' as component, 'interaction' as source, cast(null as date) as firstCodedDate, cast(null as date) as paiddate  from orec)

union all

(select distinct clmyear, partner,lineofbusiness, patientID, 'INTERACTION' as conditiontype, hcc, 'HCC Interactions' as HCCDescription,coefficient, 'ADDED' as component , 'interaction' as source, cast(null as date) as firstCodedDate, cast(null as date) as paiddate from interactionvar)

),


final as (

select distinct
mm.year,
mm.partner, 
mm.lineofbusiness,
mm.monthcount,
mm.esrdStatus,
mm.hospiceStatus,
mm.institutionalStatus,
mm.disenrolledYear,
mm.newEnrollee,
hcc.component, 
hcc.conditiontype, 
concat(hcc.component,' ',hcc.conditiontype) as combinedTitle,
mm.patientID,
mm.age,
mm.gender,
hcc.source,
hcc.hcccode,
hcc.HCCDescription,
concat(hcc.patientID,hcccode) as memberHCCcombo,
hcc.coefficient,
mm.Original_Reason_for_Entitlement_Code_OREC,
firstCodedDate,
paiddate

from 
mm

left join
hcc
on mm.patientID = hcc.patientID
and mm.partner = hcc.partner
and mm.lineofbusiness = hcc.lineofbusiness
and mm.year = hcc.year
) 

select * from final
