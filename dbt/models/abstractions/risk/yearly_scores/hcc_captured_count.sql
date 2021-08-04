with captured as (

select distinct 
clmYear,
partner, 
lineofbusiness,
patientID, 
coefficientCategory,
count(distinct capturedHCC) as capCount
from
{{ref('hcc_captured_current_year')}}

where 
coefficientCategory is not null
and clmYear >2019

group by
clmYear,
partner, 
patientID,
coefficientCategory,
lineofbusiness
),


payments as (

select * 
from
{{source('codesets','payment_hcc_counts')}}
),


final as (

select distinct 
clmYear,
patientID,
partner, 
lineofbusiness,
a.capCount,
case when coefficientCategory ='CNA' then CNA
	 when coefficientCategory ='CND' then CND
	 when coefficientCategory ='CFA' then CFA
	 when coefficientCategory ='CFD' then CFD
	 when coefficientCategory ='CPA' then CPA
	 when coefficientCategory ='CPD' then CPD
	  else CNA end as coefficient

from
captured a

left join
payments
on a.capCount = HCCCount
and cast(clmYear as string) = year
)


select * from final


