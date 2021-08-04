
{{
  config(
       tags=["evening"]
  )
}}


with chartReviewed as (

select distinct
patientID,
ConditionCategoryCode,
SupportingEvidenceName,
notes

from
{{ref('risk_chart_review')}}

where
safe.PARSE_DATE('%m/%d/%Y', reviewDate) > PARSE_DATE('%m/%d/%Y', '4/7/2021')
),


currentTable as (

select a.*
from
{{ ref('all_suspects') }}  a

inner join
   (select
   max(runDate) as maxrunDate
   from
   {{ ref('all_suspects') }}
   )
   on  runDate = maxrunDate
),


chartReviewOppty as (

select
patientId,
conditionCategoryCode,
conditionStatus

from
currentTable

where
conditionStatus = 'NO RECORDS'
),


all_suspects_ranked as (

select distinct
a.patientID,
a.partner,
a.lineOfBusiness,
a.conditiontype,
case when a.hccClosedFlag = 'no' and a.suspectHccElig = 'no' then 'INELIGIBLE'
     when a.hccClosedFlag = 'no' and a.suspectHccElig = 'yes' then 'OPEN'
     when a.hccClosedFlag = 'yes' and a.suspectHccElig = 'no' then 'CLOSED'
     when a.hccClosedFlag = 'yes' and a.suspectHccElig = 'yes' then 'CLOSED'
          else 'OPEN' end as conditionStatus,
a.ConditionSource,
cast(HCC_Rollup as string) as HCCRollup,
a.conditionCategory,
trim(cast (a.ConditionCategoryCode as string)) as ConditionCategoryCode,
ifnull( cast(HCC_Rollup as string) ,a.ConditionCategoryCode) as code,
supportingEvidenceValue,
lower(a.SupportingEvidenceSource) as SupportingEvidenceSource,
case when b.SupportingEvidenceName is not null then b.notes
     else b.SupportingEvidenceName
          end as SupportingEvidenceName,
ConditionName,
ifnull(a.SupportingEvidenceName,ConditionName) as ConditionEvidence,
SupportingEvidenceCode,
SupportingEvidenceCodeType,
providerFirstName,
providerLastNameLegalName,
a.clmCount,
a.clmLatest,
a.underlyingDxCode,
a.runDate

from
currentTable a

left join
chartReviewed b

on a.patientID = b.patientID
and a.ConditionCategoryCode = b.ConditionCategoryCode

left join
chartReviewOppty c
on a.patientID = c.patientID
and a.ConditionCategoryCode = c.ConditionCategoryCode

),


--member and lob info
current_member as (
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
    and isHARP = false
)
,


prov as (
select distinct
partner,
lineOfBusiness,
top_code.patientId,
conditionName ,
top_code.conditionType,
top_code.conditionStatus,
top_code.conditionCategory,
top_code.conditionCategoryCode,
top_code.underlyingDxCode,
recaptureEvidence,
chartReviewEvidence,
pharmEvidence,
labEvidence,
claimsEvidence,
otherEvidence,
riskFactors,
runDate
from
(select distinct
partner,
patientId,
lineOfBusiness,
conditionName,
conditionCategory,
conditionType,
conditionStatus,
ifnull( cast(HCCRollup as string) ,conditionCategoryCode) as HCCRollup,
conditionCategoryCode,
underlyingDxCode,
maxCount,
runDate
FROM
(select a.*,
ROW_NUMBER() OVER(PARTITION BY patientId,
ifnull( cast(HCCRollup as string) ,conditionCategoryCode)
ORDER BY safe_cast(conditionCategoryCode as numeric), conditionType,  conditionStatus desc, ConditionSource, clmCount desc, ifnull((concat(providerFirstName,' ',  providerLastNameLegalName )),'z') ) AS maxCount
   from
all_suspects_ranked  a
where conditionStatus <>'INELIGIBLE')  a
where maxCount = 1
order by patientId, conditionName, maxCount
) top_code

left join
(select
patientId,
code,
conditiontype ,
REPLACE(replace(LTRIM(rtrim(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(replace(trim(concat(supportingEvidence1 ,'; ' ,supportingEvidence2,'; ' ,supportingEvidence3,'; ' ,supportingEvidence4,'; ' ,supportingEvidence5)),'; ;',''),';  ;  ;  ;',''),';  ;  ;',''),  ';  ; ',''),';'),';'),'; ' ),'; '),'  ;',';'),' ,','')  as recaptureEvidence
from
      (
      SELECT distinct
      patientId,
      code,
      min(conditiontype) as conditiontype,
         max(IF(newRowNum = 1, concat(trim(ifnull(conditionEvidence,' ') ), ' ', ifnull(supportingEvidenceValue,' '),ifnull(underlyingDxCode,''),', last documented ', ifnull(cast(clmLatest as string),'date unknown') ,' by ', ifnull(provname ,'an unknown provider')), '; ') ) as supportingEvidence1,
         max(IF(newRowNum = 2, concat(trim(ifnull(conditionEvidence,' ') ), ' ', ifnull(supportingEvidenceValue,' '),ifnull(underlyingDxCode,''),', last documented ', ifnull(cast(clmLatest as string),'date unknown') ,' by ', ifnull(provname ,'an unknown provider')), '; ') ) as supportingEvidence2,
         max(IF(newRowNum = 3, concat(trim(ifnull(conditionEvidence,' ') ), ' ', ifnull(supportingEvidenceValue,' '),ifnull(underlyingDxCode,''),', last documented ', ifnull(cast(clmLatest as string),'date unknown') ,' by ', ifnull(provname ,'an unknown provider')), '; ') ) as supportingEvidence3,
         max(IF(newRowNum = 4, concat(trim(ifnull(conditionEvidence,' ') ), ' ', ifnull(supportingEvidenceValue,' '),ifnull(underlyingDxCode,''),', last documented ', ifnull(cast(clmLatest as string),'date unknown') ,' by ', ifnull(provname ,'an unknown provider')), '; ') ) as supportingEvidence4,
         max(IF(newRowNum = 5, concat(trim(ifnull(conditionEvidence,' ') ), ' ', ifnull(supportingEvidenceValue,' '),ifnull(underlyingDxCode,''),', last documented ', ifnull(cast(clmLatest as string),'date unknown') ,' by ', ifnull(provname ,'an unknown provider')), '; ') ) as supportingEvidence5,
         max(IF(newRowNum = 6, concat(trim(ifnull(conditionEvidence,' ') ), ' ', ifnull(supportingEvidenceValue,' '),ifnull(underlyingDxCode,''),', last documented ', ifnull(cast(clmLatest as string),'date unknown') ,' by ', ifnull(provname ,'an unknown provider')), '; ') ) as supportingEvidence6
         from
            (
select a.* ,
ROW_NUMBER() OVER(PARTITION BY patientID, code ORDER BY patientID,code desc, ConditionCategoryCode, conditiontype,ConditionSource ) AS newRowNum
from
(
select distinct
patientId,
conditiontype,
ConditionSource,
underlyingDxCode,
code,
conditionCategoryCode,
supportingEvidenceValue,
conditionEvidence,
provname,
clmLatest
from
---MaxClaimCount
(select * from
            (select distinct
            evid.*
            , 1 as evidencerow
            ,'most coded' as whyrank
            from
            (SELECT distinct
            patientID,
            conditiontype,
            ConditionSource,
            ifnull( cast(HCCRollup as string) ,ConditionCategoryCode) as code,
            ConditionCategoryCode,
            supportingEvidenceValue,
            ifnull(SupportingEvidenceName,ConditionName) as ConditionEvidence,
            (concat(providerFirstName,' ', providerLastNameLegalName)) as provname,
            clmLatest,
            underlyingDxCode,
            ROW_NUMBER() OVER(PARTITION BY patientId, ifnull( HCCRollup,conditionCategoryCode)
             ORDER BY conditionType,  conditionStatus desc, ConditionSource, conditionCategoryCode, clmCount desc, ifnull((concat(providerFirstName,' ', providerLastNameLegalName)) ,'z') ) AS maxCount
            from
            all_suspects_ranked
            where conditiontype = 'PERS'
            order by patientID, ConditionCategoryCode
          ) evid
          where maxCount =1
          )


union distinct

---MaxClaimCount
            (select distinct
            evid.*
            , 2 as evidencerow
            ,'most recent' as whyrank
            from
            (SELECT distinct
            patientID,
            conditiontype,
            conditionSource,
            ifnull( cast(HCCRollup as string) ,conditionCategoryCode) as code,
            conditionCategoryCode,
            supportingEvidenceValue,
            ifnull(supportingEvidenceName,conditionName) as conditionEvidence,
            (concat(providerFirstName,' ', providerLastNameLegalName)) as provName,
            clmLatest,
            underlyingDxCode,
            ROW_NUMBER() OVER(PARTITION BY patientID,  ifnull( cast(HCCRollup as string) ,conditionCategoryCode) ORDER BY patientID, ConditionCategoryCode, clmLatest desc, ifnull((concat(providerFirstName,' ',  providerLastNameLegalName )),'z') ) AS maxCount
            FROM all_suspects_ranked
            where conditiontype = 'PERS'
            order by patientID, ConditionCategoryCode
          ) evid
          where maxCount =1
          )

) a
order by
patientid, ConditionCategoryCode

             ) a
)

      GROUP BY
      patientID,
      code
)
) supporting
on
trim(cast(top_code.patientID as string)) = trim(cast(supporting.patientId as string))
and
trim(cast(top_code.HCCRollup as string)) =  trim(cast(supporting.code as string))



--Risk Factors
left outer join
(select
patientID ,
code,
replace(LTRIM(rtrim(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(replace(trim( concat('Risk Factor(s): ', SupportingEvidence1 ,'; ' ,SupportingEvidence2, '; ' ,SupportingEvidence3, '; '  , SupportingEvidence4, '; ',SupportingEvidence5)),'; ;',''),';  ;  ;  ;',''),';  ;  ;',''),  ';  ; ',''),';'),';'),'; ' ),'; '),'  ;',';')  as riskFactors,
from
      (
      SELECT distinct
      patientID,
      code,
      max(IF(newRownum = 1, concat(ConditionEvidence,' ', SupportingEvidenceSource ), '; ') ) as SupportingEvidence1,
      max(IF(newRownum = 2, concat(ConditionEvidence,' ', SupportingEvidenceSource ), '; ') ) as SupportingEvidence2,
      max(IF(newRownum = 3, concat(ConditionEvidence,' ', SupportingEvidenceSource ), '; ') ) as SupportingEvidence3,
      max(IF(newRownum = 4, concat(ConditionEvidence,' ', SupportingEvidenceSource ), '; ') ) as SupportingEvidence4,
      max(IF(newRownum = 5, concat(ConditionEvidence,' ', SupportingEvidenceSource ), '; ') ) as SupportingEvidence5
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
            ifnull( cast(HCCRollup as string) ,ConditionCategoryCode) as code,
            SupportingEvidenceCodeType,
            SupportingEvidenceSource,
            ifnull(SupportingEvidenceName,ConditionName) as ConditionEvidence

            from all_suspects_ranked
            where lower(SupportingEvidenceSource) like '%risk%')
            )
            where newRownum <6
      GROUP BY
      patientID
      , code
      )
) risk
on
top_code.patientID = risk.patientID
and
cast(top_code.HCCRollup as string)=  cast(risk.code as string)



--chart review
left outer join
(select
patientID ,
code,
replace(LTRIM(rtrim(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(replace(trim( concat('Chart Review Notes(s): ', SupportingEvidence1 ,'; ' ,SupportingEvidence2, '; ' ,SupportingEvidence3, '; '  , SupportingEvidence4, '; ',SupportingEvidence5)),'; ;',''),';  ;  ;  ;',''),';  ;  ;',''),  ';  ; ',''),';'),';'),'; ' ),'; '),'  ;',';')  as chartReviewEvidence,
from
      (
      SELECT distinct
      patientID,
      code,
      max(IF(newRownum = 1, concat(ConditionEvidence,' based on ', SupportingEvidenceSource ), '; ') ) as SupportingEvidence1,
      max(IF(newRownum = 2, concat(ConditionEvidence,' based on ', SupportingEvidenceSource ), '; ') ) as SupportingEvidence2,
      max(IF(newRownum = 3, concat(ConditionEvidence,' based on ', SupportingEvidenceSource ), '; ') ) as SupportingEvidence3,
      max(IF(newRownum = 4, concat(ConditionEvidence,' based on ', SupportingEvidenceSource ), '; ') ) as SupportingEvidence4,
      max(IF(newRownum = 5, concat(ConditionEvidence,' based on ', SupportingEvidenceSource ), '; ') ) as SupportingEvidence5
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
            ifnull( cast(HCCRollup as string) ,ConditionCategoryCode) as code,
            SupportingEvidenceCodeType,
            SupportingEvidenceSource,
            ifnull(SupportingEvidenceName,ConditionName) as ConditionEvidence

            from
            all_suspects_ranked

            where
            lower(SupportingEvidenceSource) like '%chart review%')
            )
            where newRownum <6
      GROUP BY
      patientID
      , code
      )
) review
on
top_code.patientID = review.patientID
and
cast(top_code.HCCRollup as string)=  cast(review.code as string)



--pharmacy suspects
left outer join
(select
patientID,
code,
replace(LTRIM(rtrim(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(replace(trim( concat('Perscription(s): ', SupportingEvidence1 ,'; ' ,SupportingEvidence2, '; ' ,SupportingEvidence3, '; '  , SupportingEvidence4, '; ',SupportingEvidence5)),'; ;',''),';  ;  ;  ;',''),';  ;  ;',''),  ';  ; ',''),';'),';'),'; ' ),'; '),'  ;',';')  as pharmEvidence,
from
      (
      SELECT distinct
      patientID,
      code,
      max(IF(newRownum = 1, concat(ifnull(trim(ConditionEvidence),concat('Review perscriptions: NDC ',SupportingEvidenceCode,ifnull(SupportingEvidenceName,''),' on date ',ifnull(clmLatest,'')))), '; ') ) as SupportingEvidence1,
      max(IF(newRownum = 2, concat(ifnull(trim(ConditionEvidence),concat('Review perscriptions: NDC ',SupportingEvidenceCode,ifnull(SupportingEvidenceName,''),' on date ',ifnull(clmLatest,'')))), '; ') ) as SupportingEvidence2,
      max(IF(newRownum = 3, concat(ifnull(trim(ConditionEvidence),concat('Review perscriptions: NDC ',SupportingEvidenceCode,ifnull(SupportingEvidenceName,''),' on date ',ifnull(clmLatest,'')))), '; ') ) as SupportingEvidence3,
      max(IF(newRownum = 4, concat(ifnull(trim(ConditionEvidence),concat('Review perscriptions: NDC ',SupportingEvidenceCode,ifnull(SupportingEvidenceName,''),' on date ',ifnull(clmLatest,'')))), '; ') ) as SupportingEvidence4,
      max(IF(newRownum = 5, concat(ifnull(trim(ConditionEvidence),concat('Review perscriptions: NDC ',SupportingEvidenceCode,ifnull(SupportingEvidenceName,''),' on date ',ifnull(clmLatest,'')))), '; ') ) as SupportingEvidence5
      from
            (SELECT distinct
            patientID,
            code,
            ConditionEvidence,
            SupportingEvidenceCode,clmLatest, SupportingEvidenceName,
            ROW_NUMBER() OVER(PARTITION BY patientID, code ORDER BY patientID, code,ConditionEvidence,clmLatest desc,SupportingEvidenceName nulls last)  AS newRownum
            from
            ( SELECT distinct
            patientID, max(SupportingEvidenceCode) as SupportingEvidenceCode ,
            ifnull( cast(HCCRollup as string) ,ConditionCategoryCode) as code,
            clmLatest,
            trim(SupportingEvidenceName) as ConditionEvidence,
            SupportingEvidenceName
            FROM  all_suspects_ranked
            where  conditiontype = 'SUSP'
            and lower(SupportingEvidenceSource) like '%pharmacy%'
            group by patientID,clmLatest,
            ifnull( cast(HCCRollup as string) ,ConditionCategoryCode) ,
            trim(SupportingEvidenceName) ,SupportingEvidenceName
            )
            order by patientID
            )
            where newRownum <6
      GROUP BY
      patientID
      , code
      )
) pharm
on
top_code.patientID = pharm.patientID
and
cast(top_code.HCCRollup as string)=  cast(pharm.code as string)


--lab suspects
left outer join
(select
patientId,
code,
replace(LTRIM(rtrim(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(replace(trim( concat('Lab Evidence: ', supportingEvidence1 ,'; ' , supportingEvidence2, '; ' , supportingEvidence3, '; '  , supportingEvidence4, '; ',supportingEvidence5)),'; ;',''),';  ;  ;  ;',''),';  ;  ;',''),  ';  ; ',''),';'),';'),'; ' ),'; '),'  ;',';')  as labEvidence,
from
      (
      SELECT distinct
      patientId,
      code,
      max(IF(newRownum = 1, concat(conditionEvidence,' value ', ifnull(supportingEvidenceValue,'unknown') ,' on date ',ifnull(clmLatest,'')), '; ') ) as supportingEvidence1,
      max(IF(newRownum = 2, concat(conditionEvidence,' value ', ifnull(supportingEvidenceValue,'unknown') ,' on date ',ifnull(clmLatest,'')), '; ') ) as supportingEvidence2,
      max(IF(newRownum = 3, concat(conditionEvidence,' value ', ifnull(supportingEvidenceValue,'unknown') ,' on date ',ifnull(clmLatest,'')), '; ') ) as supportingEvidence3,
      max(IF(newRownum = 4, concat(conditionEvidence,' value ', ifnull(supportingEvidenceValue,'unknown') ,' on date ',ifnull(clmLatest,'')), '; ') ) as supportingEvidence4,
      max(IF(newRownum = 5, concat(conditionEvidence,' value ', ifnull(supportingEvidenceValue,'unknown') ,' on date ',ifnull(clmLatest,'') ), '; ') ) as supportingEvidence5
      from
            (SELECT distinct
            patientID,
            conditiontype,
            ConditionSource,
            ifnull( cast(HCCRollup as string) ,conditionCategoryCode) as code,
            ConditionCategoryCode,
            SupportingEvidenceValue,
            SupportingEvidenceCodeType,
            ifnull(SupportingEvidenceName,ConditionName) as conditionEvidence,
            (concat(providerFirstName,' ', providerLastNameLegalName)) as provname,
            clmLatest,
            ROW_NUMBER() OVER(PARTITION BY patientID, ifnull( cast(HCCRollup as string) ,ConditionCategoryCode)  ORDER BY patientID, conditionCategoryCode, clmLatest desc, ifnull((concat(providerFirstName,' ', providerLastNameLegalName )),'')desc ) AS newRownum
            FROM all_suspects_ranked
            where  conditiontype = 'SUSP'
            and lower(SupportingEvidenceSource) like '%lab%'
            and lower(SupportingEvidenceSource) not like '%risk%'
            order by patientID, ConditionCategoryCode
            )
            where newRownum <6
      GROUP BY
      patientID
      , code
      )
) lab
on
top_code.patientId = lab.patientId
and
cast(top_code.HCCRollup as string)=  cast(lab.code as string)


--OTHER
left outer join
(select
patientID,
code,
replace(LTRIM(rtrim(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(replace(trim( concat('Other Evidence: ', supportingEvidence1 ,'; ' , supportingEvidence2, '; ' , supportingEvidence3, '; '  , supportingEvidence4, '; ',supportingEvidence5)),'; ;',''),';  ;  ;  ;',''),';  ;  ;',''),  ';  ; ',''),';'),';'),'; ' ),'; '),'  ;',';')  as otherEvidence,
from
      (
      SELECT distinct
      patientID,
      code,
      max(IF(newRownum = 1, concat(supportingEvidenceCodeType,' ', ConditionEvidence ), '; ') ) as supportingEvidence1,
      max(IF(newRownum = 2, concat(supportingEvidenceCodeType,' ', ConditionEvidence ), '; ') ) as supportingEvidence2,
      max(IF(newRownum = 3, concat(supportingEvidenceCodeType,' ', ConditionEvidence ), '; ') ) as supportingEvidence3,
      max(IF(newRownum = 4, concat(supportingEvidenceCodeType,' ', ConditionEvidence ), '; ') ) as supportingEvidence4,
      max(IF(newRownum = 5, concat(supportingEvidenceCodeType,' ', ConditionEvidence ), '; ') ) as supportingEvidence5
      from
            (SELECT distinct
            patientID,
            ifnull( cast(HCCRollup as string) ,ConditionCategoryCode) as code,
            SupportingEvidenceCodeType,
            SupportingEvidenceSource,
            ConditionName,
            ifnull(SupportingEvidenceName,ConditionName) as ConditionEvidence,
            ROW_NUMBER() OVER(PARTITION BY patientId, ifnull( cast(HCCRollup as string) ,ConditionCategoryCode)  ORDER BY patientId, HCCRollup ) AS newRownum
            FROM all_suspects_ranked
            where
            conditiontype = 'SUSP'
            and
            (

            lower(SupportingEvidenceSource) not like  '%claims%'
            and
            lower(SupportingEvidenceSource) not like  '%pharmacy%'
            and
            lower(SupportingEvidenceSource) not like  '%lab%')
            and
            lower(SupportingEvidenceSource) not like '%risk%'
            )
            where newRownum <6
      GROUP BY
      patientID
      , code
      )
) other
on
top_code.patientID = other.patientID
and
cast(top_code.HCCRollup as string)=  cast(other.code as string)


--claims suspects
left outer join
(select
patientID,
code,
replace(LTRIM(rtrim(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(replace(trim( concat(supportingEvidence1 ,'; ' ,supportingEvidence2, '; ' ,supportingEvidence3, '; '  , supportingEvidence4, '; ',supportingEvidence5)),'; ;',''),';  ;  ;  ;',''),';  ;  ;',''),  ';  ; ',''),';'),';'),'; ' ),'; '),'  ;',';')  as claimsEvidence,
from
      (
      SELECT distinct
      patientID,
      code,
      max(IF(newRownum = 1, concat(conditionEvidence,', evidence date ', ifnull(clmLatest,'unknown')), '; ') ) as supportingEvidence1,
      max(IF(newRownum = 2, concat(conditionEvidence,', evidence date ', ifnull(clmLatest,'unknown')), '; ') ) as supportingEvidence2,
      max(IF(newRownum = 3, concat(conditionEvidence,', evidence date ', ifnull(clmLatest,'unknown')), '; ') ) as supportingEvidence3,
      max(IF(newRownum = 4, concat(conditionEvidence,', evidence date ', ifnull(clmLatest,'unknown')), '; ') ) as supportingEvidence4,
      max(IF(newRownum = 5, concat(conditionEvidence,', evidence date ', ifnull(clmLatest,'unknown')), '; ') ) as supportingEvidence5
      from
            (SELECT distinct
            patientID,
            conditiontype,
            ConditionSource,
            code,
            ConditionEvidence,
            clmLatest,
            ROW_NUMBER() OVER(PARTITION BY patientID, code
            ORDER BY patientID, code, ConditionEvidence, clmLatest desc) AS newRownum
            from
            (select distinct   patientID, conditiontype  ,ConditionSource,
            ifnull( cast(HCCRollup as string) ,ConditionCategoryCode) as code,  ifnull(SupportingEvidenceName,ConditionName) as ConditionEvidence , max(clmLatest) as clmLatest
            FROM all_suspects_ranked
            where  conditiontype = 'SUSP'
            and lower(SupportingEvidenceSource) like '%claims%'
            and lower(SupportingEvidenceSource) not like '%risk%'
            group by
            patientID,  conditiontype  ,ConditionSource, ifnull( cast(HCCRollup as string) ,ConditionCategoryCode) ,   ifnull(SupportingEvidenceName,ConditionName)    )
            order by patientID
            )
      GROUP BY
      patientID
      , code
      )
) claims
on
top_code.patientID = claims.patientID
and
cast(top_code.HCCRollup as string)=  cast(claims.code as string)
) ,


final as (

select distinct prov.*
from prov

inner join
current_member mem
on prov.patientId = mem.id
and lower(prov.lineOfBusiness) = lower(mem.lineOfBusiness)

order by
patientID, ConditionName)

select * from final
