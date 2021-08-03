
with captureCount as (

select *

from
{{ref('hcc_captured_count')}}

where
clmYear = extract(year from current_date())
),


orec as (

select *

from
{{ref('hcc_orec')}}

where
eligYear = extract(year from current_date())

),


interactionvar_cap as (

select distinct
clmYear,
partner,
lineofbusiness,
patientID,
hcc,
coefficient

from
{{ref('hcc_interaction_variable')}}

where
clmYear = extract(year from current_date())
),


interaction_all as (

select distinct
tot.clmYear,
tot.partner,
tot.lineofbusiness,
tot.patientID,
tot.hcc,
tot.coefficient

from
{{ref('hcc_interaction_variable_breakdown')}} tot

where
tot.clmYear = extract(year from current_date())
),


interactionvar_out as (

select distinct
tot.clmYear,
tot.partner,
tot.lineofbusiness,
tot.patientID,
tot.hcc,
tot.coefficient

from
interaction_all tot

left join
interactionvar_cap cap
on tot.patientID = cap.patientID
and tot.lineofbusiness = cap.lineofbusiness
and tot.hcc= cap.hcc
and tot.clmYear = cap.clmYear

where
cap.hcc is null
),


hccsrankedpossible as (

select *

from
{{ref('hcc_ranked_possible')}}
),


demo as (

select distinct *

from
{{ ref('hcc_demo') }}

where
eligyear = extract(year from current_date())
),


mm as (

select distinct
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
)
,


hcc as (

select *

from

(select distinct partner, lineofbusiness, patientID, 'DEMO' as conditiontype, RA_Factor_Type_Code as hcccode, 'Demographic component of HCC score' as HCCDescription, coefficient, 'BASESCORE' as component ,'mmr' as source from demo)

union all

(select distinct partner, lineofbusiness, patientID, conditiontype, cast( hcc1 as string) as hcc, HCCDescription, cast(coefficient as numeric) as CoeffValue, component, source from hccsrankedpossible)

union all

(select distinct partner, lineofbusiness, patientID, 'INTERACTION' as conditiontype, 'HCC COUNT' as hcc, concat('Added ',capCount,' HCCs') as HCCDescription, coefficient, 'ADDED' as component, 'interaction' as source from captureCount)

union all

(select distinct partner, lineofbusiness, patientID, 'INTERACTION' as conditiontype, 'OREC DISABLED' as hcc, 'Originally disabled flag' as HCCDescription, coefficient, 'ADDED' as component, 'interaction' as source  from orec)

union all

(select distinct partner,lineofbusiness, patientID, 'INTERACTION' as conditiontype, hcc, 'HCC Interactions' as HCCDescription,coefficient, 'ADDED' as component , 'interaction' as source from interactionvar_cap)

union all

(select distinct partner,lineofbusiness, patientID, 'INTERACTION' as conditiontype, hcc, 'HCC Interactions' as HCCDescription,coefficient, 'OUTSTANDING' as component , 'interaction' as source from interactionvar_out)

),


final as (

select distinct
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
mm.Original_Reason_for_Entitlement_Code_OREC

from
mm

left join
hcc
on mm.patientID = hcc.patientID
and mm.partner = hcc.partner
and mm.lineofbusiness = hcc.lineofbusiness
)

select * from final
