with captured as (

select distinct
partner,
lineofbusiness,
patientID, 
coefficientCategory,
count(distinct hcc1) as capCount

from
{{ref('hcc_ranked_possible')}}

where 
coefficientCategory is not null

group by
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
)


select * from final


