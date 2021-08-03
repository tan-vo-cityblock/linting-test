--Note that hospice status is not always continuous across mmr files for an individual member

--All MMR FIles
with mmrs as (

select distinct patientID, payer, payment_date, HOSPICE from {{ ref('mmr_conn_dsnp') }}
union all
select distinct patientID, payer, payment_date, HOSPICE from {{ ref('mmr_conn_medicare') }}
union all
select distinct patientID, payer, payment_date, HOSPICE from {{ ref('mmr_emblem_medicare') }} 
union all
select distinct patientID, payer, payment_date, HOSPICE from {{ ref('mmr_tufts_duals') }}
),


--Limit MMRs to hospice
hosp as (

select distinct
mmrs.*, 
extract(year from payment_date) as paymentYear,
true as everHospice

from
mmrs 

where 
hospice is not null
and  
hospice <> ''
and 
hospice <>' '
),


--Month count hospice per year
monthCount as (

select distinct
paymentYear,
payer,
patientID,
count(distinct payment_date) as monthsHospPaymentYear

from
hosp

group by
paymentYear,
payer,
patientID

),


--most recent MMR file data
maxFileDate as (

select distinct
payer,
max(payment_date) as maxMMRdate

from 
mmrs

group by
payer
),


--most recent date member on hospice
maxHospDate as (

select distinct
patientID,
payer,
max(payment_date) as maxHospicedate

from 
hosp

group by
patientID,
payer
),


--first date member on hospice
minHospDate as (

select distinct
patientID,
payer,
min(payment_date) as minHospicedate

from 
hosp

group by
patientID,
payer
),


--member on hospice as of most recent mmr file
currentElig as (

select distinct
maxHospDate.patientID,
true as currentHospice

from
maxHospDate

inner join
maxFileDate 
on maxHospDate.maxHospicedate = maxFileDate.maxMMRdate
)


-- final table, deduped at member-year level
select distinct
 hosp.patientID
,hosp.payer
,hosp.paymentYear
,monthsHospPaymentYear
,everHospice
,currentHospice
,maxHospicedate
,minHospicedate

from
hosp

left join
currentElig
on hosp.patientID = currentElig.patientID

left join
minHospDate
on hosp.payer = minHospDate.payer
and hosp.patientID = minHospDate.patientID

left join
maxHospDate
on hosp.payer = maxHospDate.payer
and hosp.patientID = maxHospDate.patientID

left join 
monthCount
on hosp.patientID = monthCount.patientID
and hosp.payer = monthCount.payer
and hosp.paymentYear = monthCount.paymentYear

where 
hosp.PatientID is not null

