with current_member as (

select distinct * 

from 
{{ ref('hcc_demo_MYFY') }}

),


risk_claims_patients as (

select *
from
{{ref('risk_claims_patients')}}

where 
-- --MY20
(partner = 'connecticare' 
 and evidenceDate between '2019-01-01' and '2019-12-31')
or
(partner <> 'connecticare' 
and evidenceDate between '2019-01-01' and '2019-12-31'
and paidDate is not null)



-- --FY20
-- evidenceDate between '2019-01-01' and '2019-12-31'
-- and 
-- paidDate between '2019-01-01' and '2020-06-01'

--IN21
-- evidenceDate between '2019-07-01' and '2020-07-01'
-- and 
-- paidDate between '2019-07-01' and '2020-09-01'

-- --MY21
--evidenceDate between '2020-01-01' and '2020-12-31'
-- and 
-- paidDate between '2020-01-01' and '2021-03-01'


),

everoutstanding as (

select distinct 

evidenceYear,
suspcombo,  
conditionType 

from 
{{ref('hcc_ever_outstanding')}} 
),


HCC_coeff_xwalk_v24 as (

select distinct
replace(variable, "HCC","") as code,	
CNA,	
CND,	
CFA,	
CFD,	
CPA,	
CPD,	
Institutional,	
Year

from 
{{ source('codesets', 'hcc_coefficient_values') }}

where
year = '2019'
),


diag_history as (

select distinct
a.patientID, 
a.lineOfBusiness, 
a.partner,
evidenceDate as serviceDateFrom,
encounterType,
dxCode,
dxDescription,
hcc as capturedHCC,
hccRollup,
HCCDescription,
memhcccombo,
coefficientCategory

from
current_member a

inner join
risk_claims_patients clm
on a.patientID = clm.patientID

where
(claimType in ("professional" ,"facility") 
and hccEligibleCpt = true
and hccEligibleBilltype = true
and hccEligibleSpecialist = true
and hcc is not null)

or 

(claimType =  "ccd" and ( cityblockProviderType <>'BHS' or cityblockProviderType is null) 
and evidenceDate> current_date -90
and hcc is not null)


),


firstcoded as (

select distinct 
memhcccombo as capCombo,
dxs.patientID,
hccRollup,
capturedHCC,
HCCDescription, 
min(serviceDateFrom) as firstCodedDate,
lineOfBusiness,
partner,
coefficientCategory

from
diag_history dxs

group by
memhcccombo,
dxs.patientID, 
hccRollup,
capturedHCC, 
lineOfBusiness, 
partner, 
HCCDescription,
coefficientCategory
),


rolled as (

select distinct
a.*, 
dense_rank() over (partition by 
	 a.patientId, hccRollup order by patientId, hccRollup, cast(capturedHCC as numeric) nulls first) dr2

from
firstcoded a
),


preranked as (

select distinct
rolled.partner, 
rolled.lineofbusiness,
rolled.patientId,
coefficientCategory,
capturedHCC, 
HCCDescription,
case when coefficientCategory ='CND' then CND
	 when coefficientCategory ='CFA' then CFA
	 when coefficientCategory ='CFD' then CFD
	 when coefficientCategory ='CPA' then CPA
	 when coefficientCategory ='CPD' then CPD
	 else CNA
	   end as capturedCoefficient, 
'CAPTURED' as component,
capCombo, 
case when conditionType is null then 'NEW' else conditionType end as conditionType,
dense_rank() over (partition by patientId, capturedHCC order by case when conditiontype ='PERS' then 1 when conditionType = 'SUSP' then 2 when conditionType = 'NEW' then 3  else 4 end) ranked

from
rolled

left join
everoutstanding
on capCombo = suspcombo

left join
HCC_coeff_xwalk_v24 hccs
on capturedHCC = hccs.code 

where 
dr2 = 1 or hccRollup is null
),

captured as (

select distinct * 

from
preranked

where 
ranked = 1
)

select * from captured