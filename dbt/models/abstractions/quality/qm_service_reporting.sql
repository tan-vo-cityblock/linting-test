
-- patient included

with pats as (

SELECT distinct
patientID,
case when lower(partnerName) like '%emblem%' then 'emblem'
     when lower(partnerName) like '%conn%' then 'connecticare'
     else lower(partnerName)
          end as partnerName,
lineOfBusinessGrouped as lineOfBusiness,
eligYear,
eligDate,
isCityblockMemberMonth,
'1' as patJoinField

from 
{{ ref('master_member_v1')}}

where 
lineOfBusinessGrouped <> 'Not Yet Labeled' 
and eligDate  >= parse_date('%m/%d/%Y',concat('11/30/',extract (year from current_date())-1))
and eligYear <= extract (year from current_date()) 
),


-- calender dates included

cal as (

select distinct
dateDay as firstWeekOfMonth,
firstDayOfMonth,
year as reportingYear,
'1' as joinField

from 
{{ ref('date_details')}}

where
dateDay  >= parse_date('%m/%d/%Y',concat('11/30/',extract (year from current_date())-1))
and year <= extract (year from current_date())
and dayOfWeek = 1
),


--all patients reporting dates

patients as (

select distinct 
patientID,
partnerName,
lineOfBusiness,
CAST(firstDayOfMonth AS TIMESTAMP)  as reportingMonth,
CAST(firstWeekOfMonth AS TIMESTAMP)  as reportingWeek,
reportingYear

from
pats

left join
cal
on 
pats.patJoinField = cal.joinField
),


--Elig Month

Month as (

SELECT distinct
patientID,
lineOfBusiness,
eligDate,
true as eligMonthFlag

from 
pats

where 
isCityblockMemberMonth = true
),


--Elig now

Now as (

SELECT distinct
patientID,
partnerName,
lineOfBusiness,
true as eligNowFlag

from 
pats

where 
eligdate > current_date - 90 
and isCityblockMemberMonth = true
),


--Tag Enrollment for Emmblem

April as (

SELECT distinct
patientId, 
true as consentedOrEnrolledByApril

FROM 
{{ ref('member_states')}}

where
(cast(consentedAt as date) between parse_date('%m/%d/%Y',concat('01/01/',extract (year from current_date()))) and parse_date('%m/%d/%Y',concat('04/01/',extract (year from current_date())))
or 
cast(enrolledAt as date) between parse_date('%m/%d/%Y',concat('01/01/',extract (year from current_date()))) and parse_date('%m/%d/%Y',concat('04/01/',extract (year from current_date())))
)
and currentState in ( 'consented', 'enrolled')
),


--Patient list with elig flags

patient as (

select a.* , 
case when a.partnerName = 'emblem' then true else b.eligMonthFlag end as eligMonthFlag, 
--b.eligMonthFlag,
c.eligNowFlag

from 
patients a

left join 
Month b
on a.patientID = b.patientID
and a.lineofBusiness = b.lineofBusiness
and extract(date from a.reportingMonth) = b.eligDate

left join 
Now c
on a.patientID = c.patientID
and a.lineofBusiness = c.lineofBusiness
),


--Denom from QM Service

denomQM as (

select 
mem_meas.memberId,	
timestamp(date(setOn)) as setOn,
extract(year from (timestamp(date(setOn))) ) as setYear,
TIMESTAMP_TRUNC(timestamp(date(setOn)), WEEK, 'EST') as setweek,
TIMESTAMP_TRUNC(timestamp(date(setOn)), month, 'EST') as setmonth,
lower(status.name) as statusname,
meas.name as measurename

FROM
{{ source('quality_measure_mirror', 'member_measure') }} mem_meas

left join 
{{ source('quality_measure_mirror', 'measure') }} meas
on mem_meas.measureId = meas.id

left join
{{ source('quality_measure_mirror', 'member_measure_source') }} sources
on mem_meas.sourceId = sources.id

left join 
{{ source('quality_measure_mirror', 'member_measure_status') }} status
on mem_meas.statusId = status.id

left join 
{{ source('quality_measure_mirror', 'market_measure') }} mart_meas
on meas.id = mart_meas.measureId 

where status.name <> 'excluded'
and extract(year from (timestamp(date(setOn))) )  = extract(year from current_date()) 
),


--Total denom from Able

denomAble as (

select distinct
mem_meas.patientID as memberID,	
timestamp(date(createdAt)) as setOn,
extract(year from (timestamp(date(createdAt))) ) as setYear,
TIMESTAMP_TRUNC(createdAt, WEEK) as setweek,
TIMESTAMP_TRUNC(createdAt, month) as setmonth,
lower(opportunityStatus) as statusname,
case when measureID like '%FV%' 
then "Flu Vaccinations for Adults" 
else measureIdName end as measurename,
measureID,
true as ableDenomFlag

FROM 
{{ ref('hedis_gaps')}} mem_meas

where 
extract(year from (timestamp(date(createdAt))) ) = extract(year from current_date())
),


--All active measures from qm and Able

measures as (

select distinct 
memberId,
setOn,
setYear,
setMonth,
setWeek,
statusname,
measurename

FROM 
denomQM

union distinct

select distinct
memberId,
setOn,
setYear,
setMonth,
setWeek,
statusname,
measurename

FROM 
denomAble

where 
(measureID like '%TRC%'
or measureID like '%FV%'
or measureID like '%COA%')
),


--Patients in Denom reporting Year

alldenom as (
select distinct
patientID,
partnerName,
lineOfBusiness,
reportingYear,
reportingMonth,
reportingWeek,
eligMonthFlag, 
eligNowFlag,
measurename

FROM  
patient

inner join 
measures
on patient.patientID = measures.memberID 
and patient.reportingYear = measures.setYear
),


--min closed measure

minclosed as (
select distinct
memberId,	
min(setOn) as minclosed,
measurename

FROM 
measures

where 
statusname = 'closed'

group by 
memberId,	
measurename
),


--min prov closed measure

minProvclosed as (
select distinct
memberId,
min(setOn) as minclosed,
measurename

FROM 
measures

where 
statusname = 'provclosed'

group by 
memberId,	
measurename
),


--min any closed measure

minAllClosed as (
select distinct
memberId,	
min(setOn) as minclosed,
measurename

FROM 
measures

where 
lower(statusname) like '%closed%'

group by 
memberId,	
measurename
)



--final join
select distinct
alldenom.* ,
ableDenomFlag,
reportingYear as denomYear,
case when consentedOrEnrolledByApril = true then true 
    else false 
       end as consentedEnrolledByApril,
extract(month from reportingMonth) as denomMonth,
minclosed.minclosed  as minClosedOnly,
minProvClosed.minclosed as minClosedProv,
minAllClosed.minclosed as minClosedAny

from
alldenom 

left join
April
on alldenom.patientId = April.patientId

left join
denomAble
on alldenom.patientId = denomAble.memberId
and alldenom.measurename = denomAble.measurename
and extract(date from alldenom.reportingWeek) = DATE_TRUNC( extract(date from denomAble.setOn), week)


left join
minAllClosed
on alldenom.patientId = minAllClosed.memberId
and alldenom.measurename = minAllClosed.measurename
and extract(date from alldenom.reportingWeek)  = DATE_TRUNC( extract(date from minAllClosed.minclosed), week)

left join
minClosed
on alldenom.patientId = minClosed.memberId
and alldenom.measurename = minClosed.measurename
and extract(date from alldenom.reportingWeek) = DATE_TRUNC( extract(date from minClosed.minclosed), week)

left join
minProvClosed
on alldenom.patientId = minProvClosed.memberId
and alldenom.measurename = minProvClosed.measurename
and  extract(date from alldenom.reportingWeek) = DATE_TRUNC( extract(date from minProvClosed.minclosed), week)

