
with demo as (

select * from {{ref('hcc_demo')}}
),

orec as (

select distinct
eligYear,
patientID,
partner, 
lineofbusiness,
case 
when coefficientCategory ='CNA' and gender = 'F' and eligYear in (2020, 2021) then 0.250
when coefficientCategory ='CFA' and gender = 'F' and eligYear in (2020, 2021) then 0.173
when coefficientCategory ='CPA' and gender = 'F' and eligYear in (2020, 2021) then 0.136

when coefficientCategory ='CNA' and gender = 'M' and eligYear in (2020, 2021) then 0.147
when coefficientCategory ='CFA' and gender = 'M' and eligYear in (2020, 2021) then 0.182
when coefficientCategory ='CPA' and gender = 'M' and eligYear in (2020, 2021) then 0.083

when coefficientCategory ='CNA' and gender = 'F' and eligYear = 2019 then 0.248
when coefficientCategory ='CFA' and gender = 'F' and eligYear = 2019 then 0.168
when coefficientCategory ='CPA' and gender = 'F' and eligYear = 2019 then 0.136

when coefficientCategory ='CNA' and gender = 'M' and eligYear = 2019 then 0.146
when coefficientCategory ='CFA' and gender = 'M' and eligYear = 2019 then 0.180
when coefficientCategory ='CPA' and gender = 'M' and eligYear = 2019 then 0.080
     end as coefficient

from
demo a

where 
Original_Reason_for_Entitlement_Code_OREC = '1'
and coefficientCategory in ('CPA', 'CFA','CNA')
)

select * from orec
