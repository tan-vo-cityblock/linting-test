--table import

with captured as (

select
clmYear	,
partner,
lineofbusiness,
patientId,
coefficientCategory,
capturedHCC,
firstCodedDate as evidenceDate

from
{{ref('hcc_captured_current_year')}}
),


InteractionFactorsMedAdvan as (

select distinct *
from
{{ source('codesets', 'InteractionFactorsMedAdvan') }}
),


hcc_demo as (

select *
from
{{ref('hcc_demo')}}
),


--CONDITIONS

CHF as (

select distinct
clmYear,
patientID,
evidenceDate

from
captured

where
capturedHCC = '85'
),


COPD as (

select distinct
clmYear,
patientID,
evidenceDate

from
captured

where
capturedHCC in ( '110','111', '112')
),


DIABETES as (

select distinct
clmYear,
patientID,
evidenceDate

from
captured

where
capturedHCC in ( '18', '19', '17')
),


IMMUNE as (

select distinct
clmYear,
patientID,
evidenceDate

from
captured
where capturedHCC = '47'
),


CAN as (

select distinct
clmYear,
patientID,
evidenceDate

from
captured

where
capturedHCC in ( '8', '9', '10', '11', '12')
),


RENAL as (

select distinct
clmYear,
patientID,
evidenceDate

from
captured

where
capturedHCC in ( '34','35','36','37', '38')
),


CF as (

select distinct
clmYear,
patientID,
evidenceDate

from
captured

where
capturedHCC in ('82','83','84')
),


ARITHMIA as (

select distinct
clmYear,
patientID,
evidenceDate

from
captured

where
capturedHCC = '96'
),


SUD as (

select distinct
clmYear,
patientID,
evidenceDate

from
captured

where
capturedHCC in ('54','55','56')
),


PSYCH as (

select distinct
clmYear,
patientID,
evidenceDate

from
captured

where
capturedHCC in ('57','58', '59','60')
),


--INTERACTIONS

cancer as (

select distinct
clmYear,
partner, lineofbusiness, patientID,
'HCC47_GCANCER' as hcc,
IF(IMMUNE.evidenceDate > CAN.evidenceDate, IMMUNE.evidenceDate, CAN.evidenceDate) as EVIDENCEDATE

from
captured as cap

inner join
IMMUNE
using(clmYear, patientID)

inner join
CAN
using(clmYear, patientID)
),


diab_chf as (

select distinct
clmYear,
partner, lineofbusiness, patientID,
'DIABETES_CHF' as hcc,
IF(CHF.evidenceDate > DIABETES.evidenceDate, CHF.evidenceDate, DIABETES.evidenceDate) as EVIDENCEDATE

from
captured as cap

inner join
CHF
using(clmYear, patientID)

inner join
DIABETES
using(clmYear, patientID)
),


CHF_COPD as (

select distinct
clmYear,
partner, lineofbusiness, patientID,
'CHF_COPD' as hcc,
IF(CHF.evidenceDate > COPD.evidenceDate, CHF.evidenceDate, COPD.evidenceDate) as EVIDENCEDATE


from
captured as cap

inner join
CHF
using(clmYear, patientID)

inner join
COPD
using(clmYear, patientID)
),


HCC85_RENAL as (

select distinct
clmYear,
partner, lineofbusiness, patientID,
'HCC85_RENAL' as hcc,
IF(CHF.evidenceDate > RENAL.evidenceDate, CHF.evidenceDate, RENAL.evidenceDate) as EVIDENCEDATE

from
captured as cap

inner join
CHF
using(clmYear, patientID)

inner join
RENAL
using(clmYear, patientID)
),


COPD_CARD_RESP_FAIL as (

select distinct
clmYear,
partner, lineofbusiness, patientID,
'COPD_CARD_RESP_FAIL' as hcc,
IF(CF.evidenceDate > COPD.evidenceDate, CF.evidenceDate, COPD.evidenceDate) as EVIDENCEDATE

from
captured as cap

inner join
CF
using(clmYear, patientID)

inner join
COPD
using(clmYear, patientID)
),


HCC85_HCC96 as (

select distinct
clmYear,
partner, lineofbusiness, patientID,
'HCC85_HCC96' as hcc,
IF(CHF.evidenceDate > ARITHMIA.evidenceDate, CHF.evidenceDate, ARITHMIA.evidenceDate) as EVIDENCEDATE

from
captured as cap

inner join
CHF
using(clmYear, patientID)

inner join
ARITHMIA
using(clmYear, patientID)
),


SUB_PSYCH as (

select distinct
clmYear,
partner, lineofbusiness, patientID,
'SUB_PSYCH' as hcc,
IF(SUD.evidenceDate > PSYCH.evidenceDate, SUD.evidenceDate, PSYCH.evidenceDate) as EVIDENCEDATE

from
captured as cap

inner join
SUD
using(clmYear, patientID)

inner join
PSYCH
using(clmYear, patientID)
),


--Final tables

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


cat as (

select distinct
vars.*,
coefficientCategory

from
vars

left join
hcc_demo demo
on
vars.patientID = demo.patientID
and vars.clmyear = demo.eligyear
),


final as (

select distinct
clmYear,
partner,
lineofbusiness,
patientID,
hcc,
EVIDENCEDATE,
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
cat a

inner join
InteractionFactorsMedAdvan
on hcc = Variable
and clmYear = Year
)

select * from final