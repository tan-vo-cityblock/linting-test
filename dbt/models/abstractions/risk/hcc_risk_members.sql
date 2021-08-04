
---------------------------------
--- Section One: Patient Info ---
---------------------------------


--load from master member
with master_member_v1 as (

select distinct
eligYear,
eligDate,
patientId,
memberId,
medicareId,
firstname,
lastname,
gender,
dateOfBirth,
cohortName, 
cohortGoLiveDate,
case when lower(lineOfBusinessGrouped) in ('dsnp', 'duals') or lower(lineOfBusinessGrouped) like ('%medicaid%') then TRUE 
     else FALSE 
          end as medicaid, 
case when lower(partnerName) like '%emblem%' then 'emblem'
     when lower(partnerName) like '%conn%' then 'connecticare'
     else lower(partnerName)
          end as partner,
case when lower(lineOfBusinessGrouped) like '%medicaid%' then 'medicaid'
     when lower(lineOfBusinessGrouped) like '%medicare%' then 'medicare'
     when lower(lineOfBusinessGrouped) like '%dsnp%' then 'dsnp'
     when lower(lineOfBusinessGrouped) like '%dual%' then 'duals'
     when lower(lineOfBusinessGrouped) like '%commercial%' then 'commercial'
     else lower(lineOfBusinessGrouped)  
          end as lineOfBusiness,
isInLatestPayerFile,
case when lower(lineOfBusinessGrouped) = 'medicaid harp' then true
     else false end as isHARP
from 
{{ ref('master_member_v1') }}

where 
isCityblockMemberMonth = true
and 
EXTRACT(YEAR FROM eligDate) between EXTRACT(YEAR FROM current_date()) - 2 and EXTRACT(YEAR FROM current_date())
and 
(lower(lineOfBusinessGrouped) like '%medicare%' 
or lower(lineOfBusinessGrouped) like '%dsnp%' 
or lower(lineOfBusinessGrouped) like '%dual%' 
or lower(lineOfBusinessGrouped) like '%commercial%'
or lower(lineOfBusinessGrouped) like '%medicaid%'
)
and 
memberID is not null 
and 
isInLatestPayerFile = 1
),


--months eligibility count
eligibility_6_months as (

select distinct 
patientId as eligID, 
eligYear as eligYear6,
lineOfBusiness as lineOfBusinessElig,
count(distinct eligDate) as monthCount
from 
master_member_v1

group by 
patientId, 
eligYear, 
lineOfBusiness
),


--current eligibility flag
eligibility_now as (

select distinct 
patientId as nowID, 
lineOfBusiness as nowlineOfBusiness,
max(eligDate) as maxElig
from 
master_member_v1

group by 
patientId,
lineOfBusiness
),


--current delivery flag
patient_delivery_model as (

select distinct
patientId as virtualPatId, 
'yes' as virtualProgram
from 

{{ source('commons', 'patient_delivery_model') }} 
where 
currentState like '%virtual%'
and deletedAt is null
), 


--current patient state
patientState as (
select distinct
patientId as stateid, 
currentState
from
{{ ref('member_states') }} st

order by stateID
),


-----------------------------------
---Section Two: Member Inclusion---
-----------------------------------


--join indicator flags
member_included as 
(select distinct
eligYear,
patientId,
memberId,
medicareId,
gender,
firstname,
lastname,
dateOfBirth,
cohortName, 
cohortGoLiveDate,
partner,
maxElig as mostRecentEligDate,
case when maxElig > (current_date() - 90) then true 
     else false 
          end as eligNow, 
case when virtualProgram = 'yes' then true 
     else false 
          end as virtualProgram, 
currentState,
case when currentState in ('consented', 'enrolled') then true 
     else false 
          end as consentedEnrolled,
lineOfBusiness,
monthCount,
medicaid, 
case when monthCount > 5 then true 
     else false 
          end as elig6Months,

case when maxElig > (current_date() - 90)  and eligYear = extract(Year from current_date()) then true
          else false
               end as isEligCurrentYear,
isHARP
from 
master_member_v1 mm

left join
eligibility_now now
on mm.patientId = now.nowId
and mm.lineOfBusiness = now.nowlineOfBusiness

left join
eligibility_6_months elig
on  mm.patientId = elig.eligID
and mm.eligYear = elig.eligYear6
and mm.lineOfBusiness = elig.lineOfBusinessElig

left join
patient_delivery_model virtual
on mm.patientId = virtual.virtualPatId

left join
patientState st
on mm.patientId = st.stateID     
)

select distinct * from member_included