{{
  config(
    materialized='table'
  )
}}

with Raw_Facility as (
select memberidentifier.patientid as patientId
     , header.partnerClaimId as claim_id ,
     header,
     lines
     from {{ref('facility_gold_all_partners')}}
where header.date.from between date_add(date_trunc(current_date(),month),interval -16 month)   
                       and date_add(date_trunc(current_date(),month),interval -5 month)
and memberIdentifier.patientId is not null
)

, Raw_Professional as (
select memberidentifier.patientid as patientId
     , header.partnerClaimId as claim_id ,
     header,
     lines.amount.allowed as amountAllowed

      from {{ref('professional_gold_all_partners')}}
cross join unnest(lines) as lines
where lines.date.from between date_add(date_trunc(current_date(),month),interval -16 month) and date_add(date_trunc(current_date(),month),interval -5 month)
and memberIdentifier.patientId is not null
)

, Facility as (
SELECT  patientId
       , claim_id
       , code
       , tier as diagcategory
       , case when tier = 'admit' then 1
              when tier = 'principal' then 2
              when tier = 'secondary' then 3
         end as diagcatrank
       , sum(lines.amount.allowed) as allowed   
FROM Raw_Facility
cross join unnest(header.diagnoses) as diagnoses
cross join unnest(lines) as lines
group by 1,2,3,4,5
order by claim_id, tier, code
)

, Professional as (
SELECT patientId
       ,claim_id
       , code
       , tier as diagcategory
       , case when tier = 'admit' then 1
              when tier = 'principal' then 2
              when tier = 'secondary' then 3
         end as diagcatrank
       , sum(amountAllowed) as allowed   
FROM Raw_Professional
cross join unnest(header.diagnoses) as diagnoses
group by 1,2,3,4,5
order by claim_id, tier, code
)

, All_Claims as (
select distinct * from(
select * from facility 
union all 
select * from professional
)  
order by 1,2,3,4
)

, mapped_diag as (
            select a.*
                 , d.hcc_v24 as HCC_v24
            from All_Claims a
            left join (select diagnosis_code
                            , min(hcc_v24) as hcc_v24
                       from {{ source('codesets', 'hcc_2020') }}
                       group by diagnosis_code
                       ) as d
              on a.code = d.diagnosis_code
            )

, mapped_category as (
            select a.*
                   , b.hcc_descr_condensed as HCC_Group
            from mapped_diag  a
            left join {{ source('code_maps', 'HCC_Groups_2020_v24') }} as b
            on a.hcc_v24 = cast(b.hcc_v24 as string)
)

, mapped_category_conditions as (
            select a.*
                   , case when hcc_v24 in ('57', '58') then 1 else 0 end as SMI_ind
                   , case when hcc_v24 in ('54', '55', '56') then 1 else 0 end as SUD_ind
                   , case when hcc_v24 in ('59', '60') then 1 else 0 end as BH_Ind
                   , case when code = 'N186' then 1 else 0 end as ESRD_Ind
            from mapped_category a
)

, Claims_Diag_Flag_dups as (
select *, row_number() over (partition by patientId, claim_id, code order by  diagcatrank) as rankremove
from mapped_category_conditions
)

, Claims_Diag_Remove_dups_Rank as (
select *, row_number() over (partition by patientId, claim_id order by  diagcatrank, HCC_Group desc) as rank
from Claims_Diag_Flag_dups
where rankremove = 1
)

, AllDiag1 as (
select  patientId, claim_id, hcc_group, sum(allowed) as allowed, 1 as count
from Claims_Diag_Remove_dups_Rank
where 

hcc_group is not null
group by patientId, claim_id, hcc_group
order by patientId, claim_id, allowed
)

, AllDiag2 as (
select patientid, hcc_group, sum(allowed) as allowed, sum(count) as count
from allDiag1
group by patientid,  hcc_group
order by patientid, allowed desc
)

, allDiag3 as (
select *
       , row_number() over(partition by patientid order by allowed desc) as rank_allowed
       , row_number() over(partition by patientid order by  count desc, allowed desc) as rank_count
from allDiag2
)

, alldiagfinal as (
select distinct a.patientid, a.GRPHCC_DOL_All_1, b.GRPHCC_DOL_All_2, c.GRPHCC_CNT_All_1, d.GRPHCC_CNT_All_2
from
    (select distinct patientid
           , hcc_group as GRPHCC_DOL_All_1
     from allDiag3
        where rank_allowed = 1) a
left join
    (select distinct patientid
           , hcc_group as GRPHCC_DOL_All_2
     from allDiag3
      where rank_allowed = 2) b
  on a.patientId = b.patientId
left join
    (select distinct patientid
           , hcc_group as GRPHCC_CNT_All_1
     from allDiag3
    where rank_count = 1) c
  on a.patientId = c.patientId
left join
    (select distinct patientid
           , hcc_group as GRPHCC_CNT_All_2
     from allDiag3
    where rank_count = 2) d
  on a.patientId = d.patientId
)

, threeDiag1 as (
select patientid, claim_id, hcc_group, sum(allowed) as allowed, 1 as count
from Claims_Diag_Remove_dups_Rank
  where rank <= 3 and hcc_group is not null
  group by patientid, claim_id, hcc_group
  order by patientid, claim_id, allowed
)

, threeDiag2 as (
select patientid, hcc_group, sum(allowed) as allowed, sum(count) as count
from threeDiag1
  group by patientid, hcc_group
  order by patientid, allowed desc
)

, threeDiag3 as (
select *
       , row_number() over(partition by patientid  order by allowed desc) as rank_allowed
       , row_number() over(partition by patientid  order by  count desc, allowed desc) as rank_count
from threeDiag2
)

, threediagfinal as (
select distinct a.patientid, a.GRPHCC_DOL_D3_1, b.GRPHCC_DOL_D3_2, c.GRPHCC_CNT_D3_1, d.GRPHCC_CNT_D3_2
from
(select distinct patientid 
       , hcc_group as GRPHCC_DOL_D3_1
from threeDiag3
  where rank_allowed = 1) a
left join
(select distinct patientid
       , hcc_group as GRPHCC_DOL_D3_2
from threeDiag3
  where rank_allowed = 2) b
  on a.patientID = b.patientId
left join
(select distinct patientid
       , hcc_group as GRPHCC_CNT_D3_1
from threeDiag3
  where rank_count = 1) c
  on a.patientId = c.patientId
left join
(select distinct patientid
       , hcc_group as GRPHCC_CNT_D3_2
from threeDiag3
  where rank_count = 2) d
  on a.patientId = d.patientId
)

, oneDiag1 as (
select patientid, claim_id, hcc_group, sum(allowed) as allowed, 1 as count
from Claims_Diag_Remove_dups_Rank
  where rank = 1
        and hcc_group is not null
  group by patientid, claim_id, hcc_group
  order by patientid, claim_id, allowed
)

, oneDiag2 as (
select patientid, hcc_group, sum(allowed) as allowed, sum(count) as count
from oneDiag1
  group by patientid, hcc_group
  order by patientid, allowed desc
)

, oneDiag3 as (
select *
       , row_number() over(partition by patientid order by patientid, allowed desc) as rank_allowed
       , row_number() over(partition by patientid  order by patientid, count desc, allowed desc) as rank_count
from oneDiag2
)

, onediagfinal as (
select distinct a.patientid,  a.GRPHCC_DOL_D1_1, b.GRPHCC_DOL_D1_2, c.GRPHCC_CNT_D1_1, d.GRPHCC_CNT_D1_2
from
  (select distinct patientid
       , hcc_group as GRPHCC_DOL_D1_1
  from oneDiag3
      where rank_allowed = 1) a
left join
(select distinct patientid
       , hcc_group as GRPHCC_DOL_D1_2
from oneDiag3
  where rank_allowed = 2) b
  on a.patientId = b.patientId
left join
(select distinct patientid
       , hcc_group as GRPHCC_CNT_D1_1
from oneDiag3
  where rank_count = 1) c
  on a.patientId = c.patientId
left join
(select distinct patientid
       , hcc_group as GRPHCC_CNT_D1_2
from oneDiag3
  where rank_count = 2) d
  on a.patientId = d.patientId
)
, HCC as (
select a.patientid
       
       , a.GRPHCC_DOL_all_1 as group_1 --Top HCC based on dollars, using all codes
       , a.GRPHCC_DOL_all_2 as group_2 --Second HCC based on dollars, using all codes
       , b.GRPHCC_DOL_D3_1 --Top HCC based on dollars, using first 3 codes
       , b.GRPHCC_DOL_D3_2 --Second HCC based on dollars, using first 3 codes
       , c.GRPHCC_DOL_D1_1 --Top HCC based on dollars, using first 1 code
       , c.GRPHCC_DOL_D1_2 --Second HCC based on dollars, using first 1 code
       , a.GRPHCC_CNT_all_1 --Top HCC based on count of claims, using all codes
       , a.GRPHCC_CNT_all_2 --Second HCC based on count of claims, using all codes
       , b.GRPHCC_CNT_D3_1 --Top HCC based on count of claims, using top 3 codes
       , b.GRPHCC_CNT_D3_2 --Second HCC based on count of claims, using top 3 codes
       , c.GRPHCC_CNT_D1_1 --Top HCC based on count of claims, using top 1 code
       , c.GRPHCC_CNT_D1_2 --Second HCC based on count of claims, using top 1 code
from alldiagfinal a
left join threediagfinal b
  on a.patientId = b.patientId
left join onediagfinal c
  on a.patientId = c.patientId
)
select * from HCC where patientId is not null #group by 1 order by 2 desc
