{{ config(
        tags = ["evening"]
   )
}}

with review as (

select distinct *

from
{{ref('risk_chart_review')}}

where
safe.PARSE_DATE('%m/%d/%Y', reviewDate) > PARSE_DATE('%m/%d/%Y', '4/7/2021')
),


mostrecent as (

select distinct
review.* 

from
review

inner join
(select distinct 
   conditionCategoryCode,
   patientId, 
   max(createdAt) as createdAt
from review 
group by conditionCategoryCode, patientId) max

using(conditionCategoryCode,patientID,createdAt)
),


hcc_codes_2020 as (

select distinct * 

from
{{ source('codesets', 'hcc_2020') }} 

where
lower(cast(hcc_v24_2020 as string)) = 'yes'
),


--HCC description data
risk_hcc_descript as (

select distinct *

from 
{{ source('codesets', 'risk_hcc_descript') }} hcc

where 
version = 'v24'
),


--provider NPI info
npidata as (

select distinct * 

from
{{ source('nppes','npidata_20190512')}}
),


--members with required RA related columns
member as (

select distinct
patientId , 
memberId as id,
partner,
lineOfBusiness

from
{{ ref('hcc_risk_members') }}

where
isHARP = false
),


final as (

select distinct 
member.partner, 
review.patientId, 
member.lineOfBusiness,  
upper(review.conditionType) as conditionType, 
"prospective-review" as conditionSource, 
case when lower(member.lineOfBusiness) = 'medicaid' then '3M CRG'
     when lower(member.lineOfBusiness) = 'medicare' then 'HCC'
     when lower(member.lineOfBusiness) like '%dual%' then 'HCC'
     when lower(member.lineOfBusiness) = 'dsnp' then 'HCC'
     when lower(member.lineOfBusiness) = 'm' then 'HCC'
     when lower(member.lineOfBusiness) = 'commercial' then 'HHS'
           end as conditionCategory, 
review.conditionCategoryCode,
hcc.hccdescr as conditionName, 
review.conditionStatus, 
review.underlyingDxCode,  
review.confidence,  
case when review.confidence = 'VERY HIGH' then 1
     when review.confidence = 'HIGH' then 2
     when review.confidence = 'MEDIUM' then 3
     when review.confidence = 'LOW' then 4
     when review.confidence = 'VERY LOW' then 5
          end as confidenceNumeric, 
review.supportingEvidenceCode, 
review.supportingEvidenceCodeType,  
review.supportingEvidenceValue, 
case when conditionstatus in ('OPEN', 'CLOSED', 'REJECTED', 'NEW') then review.Notes
     when review.Notes is null then review.evidence
     else review.evidence
          end as supportingEvidenceName,
"chart review" as supportingEvidenceSource, 
current_date() as dateUploaded,
review.Provider_First_Name AS providerFirstName,
review.Provider_Last_Name_Legal_Name as providerLastNameLegalName, 
null as clmCount, 
coalesce(safe.parse_date("%m/%d/%Y", evidenceDate), safe.parse_date("%m/%d/%Y", reviewDate)  ) as clmLatest

from
mostrecent review

left join
member
on review.patientID = member.patientID

LEFT JOIN 
risk_hcc_descript hcc 
on review.conditionCategoryCode = hcc.hcc
)

select * from final
