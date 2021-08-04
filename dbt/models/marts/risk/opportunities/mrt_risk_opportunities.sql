
{{
config(
tags=["evening"]
)
}}


--member and lob info
with current_member as (

select distinct
patientId as id,
memberId,
eligYear,
partner,
lineOfBusiness

from
{{ ref('hcc_risk_members') }}

where
eligNow = true
and eligYear = extract(Year from current_date())
and lineOfBusiness <> 'commercial'
),


chartReviewFlag as (

select distinct
patientID,
ConditionCategoryCode,
SupportingEvidenceName,
notes,
true as hasChartReview

from
{{ref('risk_chart_review')}}

where
safe.PARSE_DATE('%m/%d/%Y', reviewDate) > PARSE_DATE('%m/%d/%Y', '4/7/2021')
),


beginTable as (

select
a.patientID,
partner,
lineOfBusiness,
conditiontype,
case when a.hccClosedFlag = 'no' and a.suspectHccElig = 'no' then 'INELIGIBLE'
     when a.hccClosedFlag = 'no' and a.suspectHccElig = 'yes' then 'OPEN'
     when a.hccClosedFlag = 'yes' and a.suspectHccElig = 'no' then 'CLOSED'
     when a.hccClosedFlag = 'yes' and a.suspectHccElig = 'yes' then 'CLOSED'
          else 'OPEN' end as conditionStatus,
ConditionSource,
cast(HCC_Rollup as string) as HCCRollup,
conditionCategory,
trim(cast (a.ConditionCategoryCode as string)) as ConditionCategoryCode,
ifnull( cast(HCC_Rollup as string) ,a.ConditionCategoryCode) as code,
supportingEvidenceValue,
lower(a.SupportingEvidenceSource) as SupportingEvidenceSource,
a.SupportingEvidenceName,
ConditionName,
ifnull(a.SupportingEvidenceName,ConditionName) as ConditionEvidence,
SupportingEvidenceCode,
SupportingEvidenceCodeType,
concat(providerFirstName,' ', providerLastNameLegalName) as provname,
clmCount,
clmLatest,
underlyingDxCode,
runDate,
closedDate,
closedSource

from
{{ ref('all_suspects') }} a

where
lineOfBusiness <> 'commercial'
and extract(year from rundate) = extract(year from current_date())
),


--most recent rundate of suspects ranked
maxDateBeginTable as (

select
max(runDate) as maxrunDate

from
beginTable
),


--Full set of opportunities
all_suspects_ranked as (

select distinct a.*

from
beginTable  a

--joined to most recent version
inner join
maxDateBeginTable
on runDate = maxrunDate
),


evid as (

SELECT distinct
patientID,
conditiontype,
conditionStatus,
ConditionSource,
code,
ConditionCategoryCode,
supportingEvidenceValue,
ConditionEvidence,
provname,
clmLatest,
underlyingDxCode,
ROW_NUMBER() OVER(PARTITION BY patientID,  ifnull( HCCRollup ,conditionCategoryCode) ORDER BY patientID, ConditionCategoryCode, clmLatest, ifnull(provname,'z') ) AS maxCount

FROM
all_suspects_ranked

where
conditiontype = 'PERS'

order by
patientID,
ConditionCategoryCode
) ,


--Most Coded Persistent Condition
mostCoded as (

select distinct
evid.*
, 1 as evidencerow
,'most coded' as whyrank

from
evid

where
maxCount =1
),


--evidence supporting most coded condition
supporting as (

select
patientId,
code,
conditiontype ,
trim(supportingEvidence1," ; ") as recaptureEvidence
from
(SELECT distinct
patientId,
code,
min(conditiontype) as conditiontype,
max(IF(newRowNum = 1, concat(ifnull(supportingEvidenceValue,''),ifnull(underlyingDxCode,''),' documention date ', ifnull(cast(clmLatest as string),'unknown') , ifnull(concat(' by ',provname) ,'')), '; ') ) as supportingEvidence1
from
(
select a.* ,
ROW_NUMBER() OVER(PARTITION BY patientID, code ORDER BY patientID,code desc, ConditionCategoryCode, conditiontype,ConditionSource ) AS newRowNum

from
(select distinct
patientId,
conditiontype,
conditionStatus,
ConditionSource,
underlyingDxCode,
code,
conditionCategoryCode,
supportingEvidenceValue,
conditionEvidence,
provname,
clmLatest

from
mostCoded a

order by
patientid,
ConditionCategoryCode
) a
)

GROUP BY
patientID,
code
)
),


--chart review suspects
review as (

select
patientID ,
code,
trim(concat('Chart Review Note(s): ', SupportingEvidence1 ,'; ' ,SupportingEvidence2, '; ' ,SupportingEvidence3)," ; ")  as chartReviewEvidence,
from
(SELECT distinct
patientID,
code,
max(IF(newRownum = 1, ConditionEvidence, '; ') ) as SupportingEvidence1,
max(IF(newRownum = 2, ConditionEvidence, '; ') ) as SupportingEvidence2,
max(IF(newRownum = 3, ConditionEvidence, '; ') ) as SupportingEvidence3
from
(SELECT distinct
patientID,
code,
SupportingEvidenceCodeType,
SupportingEvidenceSource,
ConditionEvidence,
ROW_NUMBER() OVER(PARTITION BY patientID, code ORDER BY patientID, code,SupportingEvidenceCodeType desc) AS newRownum
FROM
(select distinct
patientID,
code,
SupportingEvidenceCodeType,
SupportingEvidenceSource,
ConditionEvidence

from
all_suspects_ranked

where
SupportingEvidenceSource like '%chart review%')
)

where
newRownum <4

GROUP BY
patientID
,code
)
),


--lab suspects
lab as (

select
patientId,
code,
trim(concat('Lab Evidence: ', supportingEvidence1 ))    as labEvidence,
from
(
SELECT distinct
patientId,
code,
max(IF(newRownum = 1, concat(conditionEvidence,' value ', ifnull(supportingEvidenceValue,'NA'),' on ',ifnull(clmLatest,'') ), '; ') ) as supportingEvidence1
from
(SELECT distinct
patientID,
conditiontype,
ConditionSource,
code,
ConditionCategoryCode,
SupportingEvidenceValue,
SupportingEvidenceCodeType,
conditionEvidence,
provname,
clmLatest,
ROW_NUMBER() OVER(PARTITION BY patientID, ifnull(HCCRollup,ConditionCategoryCode)  ORDER BY patientID, conditionCategoryCode, clmLatest desc, ifnull(provname,'')desc ) AS newRownum

FROM
all_suspects_ranked

where
conditiontype = 'SUSP'
and SupportingEvidenceSource like '%lab%'
)

where
newRownum <2

GROUP BY
patientID
,code
)
),


--parm suspects
pharm as (

select
patientID,
code,
trim(concat('Rx(s): ', SupportingEvidence1))  as pharmEvidence,

from
(SELECT distinct
patientID,
code,
max(IF(newRownum = 1, concat(ifnull(trim(ConditionEvidence),concat('NDC code: ',SupportingEvidenceCode,ifnull(SupportingEvidenceName,''),' on date ',ifnull(clmLatest,'')))), '; ') ) as SupportingEvidence1

from
(SELECT distinct
patientID,
code,
clmLatest,
ConditionEvidence,
SupportingEvidenceCode,
SupportingEvidenceName,
ROW_NUMBER() OVER(PARTITION BY patientID, code ORDER BY patientID, code,ConditionEvidence, clmLatest desc, SupportingEvidenceName nulls last)  AS newRownum
from
(SELECT distinct
patientID, max(SupportingEvidenceCode) as SupportingEvidenceCode , clmLatest,
ifnull( HCCRollup ,ConditionCategoryCode) as code,
trim(SupportingEvidenceName) as ConditionEvidence,
SupportingEvidenceName

FROM
all_suspects_ranked

where
conditiontype = 'SUSP'
and SupportingEvidenceSource like '%pharmacy%'

group by
patientID,clmLatest,
ifnull( HCCRollup ,ConditionCategoryCode) ,
SupportingEvidenceName,
trim(SupportingEvidenceName) )
)

where
newRownum <2

GROUP BY
patientID
,code
)
),


--other sources of evidence
other as (

select
patientID,
code,
trim(concat('Other Evidence: ', supportingEvidence1 ,'; ' , supportingEvidence2, '; ' , supportingEvidence3)," ; ")   as otherEvidence,
from
(
SELECT distinct
patientID,
code,
max(IF(newRownum = 1, concat(ConditionEvidence," ", supportingEvidenceValue), '; ') ) as supportingEvidence1,
max(IF(newRownum = 2, concat(ConditionEvidence," ", supportingEvidenceValue), '; ') ) as supportingEvidence2,
max(IF(newRownum = 3, concat(ConditionEvidence," ", supportingEvidenceValue) , '; ') ) as supportingEvidence3
from
(SELECT distinct
patientID,
code,
SupportingEvidenceCodeType,
SupportingEvidenceSource,
ConditionName,
supportingEvidenceValue,
ConditionEvidence,
ROW_NUMBER() OVER(PARTITION BY patientId, ifnull( HCCRollup ,ConditionCategoryCode)  ORDER BY patientId, HCCRollup ) AS newRownum

FROM
all_suspects_ranked

where
conditiontype = 'SUSP'
and SupportingEvidenceSource not like '%claims%'
and SupportingEvidenceSource not like '%pharmacy%'
and SupportingEvidenceSource not like '%lab%'
and SupportingEvidenceSource not like '%risk%'
and supportingEvidenceCodeType not like '%cost%'
)

where
newRownum <4

GROUP BY
patientID
,code
)
),


--Frequency ranking
mostFreq as (

select a.*,
ROW_NUMBER() OVER(PARTITION BY patientId, ifnull( HCCRollup,conditionCategoryCode)
             ORDER BY safe_cast(conditionCategoryCode as numeric), conditionType,  conditionStatus desc, ConditionSource, clmCount desc, ifnull(provname,'z') ) AS maxCount

from
all_suspects_ranked  a
where conditionStatus <>'INELIGIBLE'
),


--Top coded code
top_code as (

select distinct
mostFreq.partner,
mostFreq.patientId,
mostFreq.lineOfBusiness,
mostFreq.conditionName,
mostFreq.conditionCategory,
mostFreq.conditionType,
mostFreq.conditionStatus,
mostFreq.closedDate,
mostFreq.closedSource,
ifnull( mostFreq.HCCRollup,mostFreq.conditionCategoryCode) as HCCRollup,
mostFreq.conditionCategoryCode,
mostFreq.underlyingDxCode,
mostFreq.maxCount,
mostFreq.runDate,
case when hasChartReview = true then true else false end as hasChartReview,
chartReviewFlag.notes as noteFromCDITeam

FROM
mostFreq

left join
chartReviewFlag
on mostFreq.patientId = chartReviewFlag.patientId
and mostFreq.conditionCategoryCode = chartReviewFlag.ConditionCategoryCode

where
maxCount = 1
),


--Final Table Assembled
prov as (

select distinct
partner,
lineOfBusiness,
top_code.patientId,
conditionName,
top_code.conditionType,
top_code.conditionStatus,
top_code.closedDate,
top_code.closedSource,
top_code.conditionCategory,
top_code.conditionCategoryCode,
top_code.underlyingDxCode,
recaptureEvidence,
chartReviewEvidence,
pharmEvidence,
labEvidence,
otherEvidence,
runDate,
noteFromCDITeam

from
top_code

--chart review (always joining to chart review)
left outer join
review
on
top_code.patientID = review.patientID
and top_code.HCCRollup = review.code

--supporting recapture evidence (only if not chart review exists)
left join
supporting
on  top_code.patientID  = supporting.patientId
and top_code.HCCRollup =  supporting.code
and hasChartReview = false

--pharmacy suspects (only if not chart review exists)
left outer join
pharm
on top_code.patientID = pharm.patientID
and top_code.HCCRollup =  pharm.code
and hasChartReview = false

--lab suspects (only if not chart review exists)
left outer join
lab
on top_code.patientId = lab.patientId
and top_code.HCCRollup = lab.code
and hasChartReview = false

--OTHER suspects (only if not chart review exists)
left outer join
other
on top_code.patientID = other.patientID
and top_code.HCCRollup =  other.code
and hasChartReview = false
),


final as (

select distinct
prov.*

from
prov

inner join
current_member mem
on prov.patientId = mem.id
and prov.lineOfBusiness = mem.lineOfBusiness

order by
patientID,
ConditionName)


SELECT distinct
partner,
lineOfBusiness,
patientId,
conditionName,
conditionType,
conditionStatus,
closedDate,
closedSource,
conditionCategory,
conditionCategoryCode,
underlyingDxCode,
replace(replace(trim(trim(concat(ifnull(chartReviewEvidence,''),'; ',ifnull(recaptureEvidence,''), '; ',  ifnull(pharmEvidence,''),'; ', ifnull(labEvidence,''),'; ', ifnull(otherEvidence,''),'; '),'; '),''),'; ;',''),' ;','') as evidence,
runDate,
noteFromCDITeam

FROM
final

