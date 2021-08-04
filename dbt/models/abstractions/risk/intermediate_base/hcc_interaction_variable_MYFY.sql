
with captured as (

select * 
from 
{{ref('hcc_captured_current_year_MYFY')}}
),


InteractionFactorsMedAdvan as (

select distinct * 
from 
{{ source('codesets', 'InteractionFactorsMedAdvan') }} 

where year = 2019
),


hcc_demo as (

select * 
from 
{{ref('hcc_demo_MYFY')}}

),


cancer as (
select distinct 
partner, lineofbusiness, patientID,
'HCC47_GCANCER' as hcc

from 
captured as cap

where
patientID in (select distinct patientID from captured where capturedHCC = '48')
and patientID in (select distinct  patientID from captured where capturedHCC in ( '8', '9', '10', '11', '12'))
),


diab_chf as (
select distinct 
partner, lineofbusiness, patientID, 
'DIABETES_CHF' as hcc

from 
captured as cap

where
patientID in (select distinct  patientID from captured where capturedHCC = '85')
and patientID in (select distinct  patientID from captured where capturedHCC in ( '18', '19', '17'))
),


CHF_COPD as (
select distinct  
partner, lineofbusiness, patientID,
'CHF_COPD' as hcc

from 
captured as cap

where 
patientID in (select distinct  patientID from captured where capturedHCC = '85')
and patientID in (select distinct  patientID from captured where capturedHCC in ( '110','111', '112'))
),


HCC85_RENAL as (
select distinct 
partner, lineofbusiness, patientID,
'HCC85_RENAL' as hcc

from 
captured as cap

where 
patientID in (select distinct  patientID from captured where capturedHCC = '85')
and patientID in (select distinct  patientID from captured where capturedHCC in ( '34','35','36','37', '38'))
),


COPD_CARD_RESP_FAIL as (

select distinct 
partner, lineofbusiness, patientID,
'COPD_CARD_RESP_FAIL' as hcc

from 
captured as cap

where 
patientID in (select distinct  patientID from captured where capturedHCC in ('82','83','84'))
and patientID in (select distinct  patientID from captured where capturedHCC in ( '110','111', '112'))
),


HCC85_HCC96 as (

select distinct 
partner, lineofbusiness, patientID,
'HCC85_HCC96' as hcc
from 
captured as cap

where 
patientID in (select distinct  patientID from captured where capturedHCC = '85')
and patientID in (select distinct  patientID from captured where capturedHCC = '96')
),


SUB_PSYCH as (

select distinct 
partner, lineofbusiness, patientID,
'SUB_PSYCH' as hcc
from 
captured as cap

where 
patientID in (select distinct  patientID from captured where capturedHCC in ('54','55', '56'))
and patientID in (select distinct  patientID from captured where capturedHCC in ('57','58', '59','60'))
),



vars as (

select * from cancer
union all
select * from diab_chf
union all
select * from CHF_COPD
union all
select * from HCC85_RENAL
union all
select * from COPD_CARD_RESP_FAIL
union all
select * from HCC85_HCC96
union all
select * from SUB_PSYCH
),


withcat as (

select distinct
vars.*,
coefficientCategory

from
vars

left join
hcc_demo demo
on 
vars.patientID = demo.patientID
),



final as (
select distinct 
a.* , 
case 
when coefficientCategory ='CNA' then CNA
when coefficientCategory ='CFA' then CFA
when coefficientCategory ='CPA' then CPA
when coefficientCategory ='CND' then CND
when coefficientCategory ='CFD' then CFD
when coefficientCategory ='CPD' then CPD
	 else CNA
end as coefficient

from
withcat a

inner join
InteractionFactorsMedAdvan
on
hcc = Variable
)


select * from final