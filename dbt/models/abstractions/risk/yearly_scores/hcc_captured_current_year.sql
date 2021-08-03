with current_member as (

select distinct * 

from 
{{ ref('hcc_demo') }}
),


--rolls up hccs to categories
hcc_rollup as (

select distinct
hcc,
hcc_rollup as capturedGroup

from
{{ source('codesets', 'hcc_rollup_2020') }}
),


risk_hcc_descript as (

select distinct
hcc.*

from
{{ source('codesets', 'risk_hcc_descript') }} as hcc

where
version = 'v24'
),


risk_claims_patients as (

select *
from
{{ref('risk_claims_patients')}}

where
(
(paidDate is not null and partner <> 'connecticare')
or 
( partner = 'connecticare')
)
--and evidenceDate between parse_date('%m/%d/%Y','1/1/2021') and parse_date('%m/%d/%Y','7/1/2021')
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
),


mor_data as (

select distinct
cast(paymentYear as numeric) as clmYear,
concat(clm.patientid,clm.hcc) as capCombo,
clm.patientid,
hccRollup as capturedGroup,
clm.hcc as capturedHCC,
hccdescr as HCCDescription,
parse_date('%m/%d/%Y',concat('12/30/',cast(extract(year from paymentDate) as string))) as serviceDateFrom,
a.lineOfBusiness,
a.partner,
coefficientCategory,
parse_date('%m/%d/%Y',concat('12/30/',cast(extract(year from paymentDate) as string))) as paidDate

from
{{ref('mor_data')}} clm

inner join
risk_hcc_descript descr
on clm.hcc = descr.hcc

inner JOIN
current_member a
on clm.patientID = a.patientID
and clm.paymentYear = cast(a.eligYear as string)

where
cast(paymentDate as string) like '%12-01%'
and clm.partner <>'tufts'
),


opptys_supplemental as (

select distinct
extract(year from clmLatest) as clmYear,
CONCAT(clm.patientID, conditionCategoryCode) as capCombo,
clm.patientID,
case when capturedGroup is null then clm.conditionCategoryCode
        else capturedGroup
        end as capturedGroup,
conditionCategoryCode AS capturedHCC,
conditionName AS HCCDescription,
parse_date('%m/%d/%Y',concat('12/31/',cast(extract(year from clmLatest) as string))) as serviceDateFrom,
clm.lineOfBusiness,
clm.partner,
coefficientCategory,
parse_date('%m/%d/%Y',concat('12/31/',cast(extract(year from clmLatest) as string))) as paidDate,

from
{{ref('opptys_supplemental')}} clm

inner JOIN
current_member a
on clm.patientID = a.patientID
and extract(year from clmLatest) = a.eligYear

left join
hcc_rollup roll
on
cast(clm.conditionCategoryCode as string) = roll.hcc

),


claims as (

select distinct
clmYear,
memhcccombo,
a.patientID,
hccRollup as capturedGroup,
hcc as capturedHCC,
HCCDescription,
evidenceDate as serviceDateFrom,
a.lineOfBusiness,
a.partner,
coefficientCategory,
paidDate

from
current_member a

inner join
risk_claims_patients clm
on a.patientID = clm.patientID
and  a.eligYear = clm.clmYear

where
(claimType in ("professional" ,"facility")  
and clmYear between extract(year from current_date())-3 and extract(year from current_date())
and hccEligibleCpt = true
and hccEligibleBilltype = true
and hccEligibleSpecialist = true
and hcc is not null)

or 

(claimType =  "ccd" and ( cityblockProviderType <>'BHS' or cityblockProviderType is null) and evidenceDate> current_date -90
and hcc is not null)


),


diag_history as(

select 'claims/ehr' as source, * from claims
union all
select 'mor' as source, *  from mor_data
union all
select 'supplemental' as source, * from opptys_supplemental
),


firstcoded as (

select *
from
(select distinct
clmYear,
memhcccombo as capCombo,
dxs.patientID,
capturedGroup,
capturedHCC,
HCCDescription, 
serviceDateFrom as firstCodedDate,
lineOfBusiness,
partner,
coefficientCategory,
source,
paidDate,
dense_rank() over (partition by clmYear, patientId, lineofbusiness,capturedHCC order by serviceDateFrom , source, paidDate ) as ranked

from
diag_history dxs

)
where ranked = 1
),


rolled as (

select distinct *

from
( select distinct
a.*, 
dense_rank() over (partition by clmYear, a.patientId, capturedGroup order by patientId, capturedGroup, cast(capturedHCC as numeric) nulls first, source) dr2

from
firstcoded a

)
where
dr2 = 1
),


preranked as (

select distinct
rolled.clmYear,
rolled.partner, 
rolled.lineofbusiness,
rolled.patientId,
coefficientCategory,
capturedHCC,
capturedGroup,
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
source,
case when conditionType is null then 'NEW' else conditionType end as conditionType,
firstCodedDate,
case when paidDate is null and partner ='connecticare' then DATE_ADD(paidDate, INTERVAL 25 DAY)
    else paidDate
        end as paidDate,
dense_rank() over (partition by clmYear,patientId, capturedHCC order by case when conditiontype ='PERS' then 1 when conditionType = 'SUSP' then 2 when conditionType = 'NEW' then 3  else 4 end,source) ranked

from
rolled

left join
everoutstanding
on capCombo = suspcombo
and cast(clmYear as string) = cast(evidenceyear as string) 

left join
HCC_coeff_xwalk_v24 hccs
on capturedHCC = hccs.code 
and cast(clmYear as string) = cast(hccs.year as string)

),


captured as (

select distinct * 

from
preranked

where
ranked = 1
)

select * from captured
