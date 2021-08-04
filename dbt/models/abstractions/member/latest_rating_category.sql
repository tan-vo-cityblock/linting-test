
with src_member as (

  select patientId
  from {{ ref('src_member') }}
),

member_index_member as (

  select
    id as patientId,
    categoryId
  from {{ source('member_index', 'member') }}
),

member_index_category as (

  select
    id as categoryId,
    name as category
  from {{ source('member_index', 'category') }}
),

-- tufts & healthy blue unioned. similar logic
tufts_member_daily as (

  select distinct
    memberIdentifier.partnerMemberId,
    dateEffective.from as dateEffectiveFrom,
    REGEXP_REPLACE(upper(coalesce(eligibility.partnerBenefitPlanId, eligibility.subLineOfBusiness)), 'UNIFY', '') as category,
    _table_suffix as fileDate
  from {{ source('tufts', 'Member_Daily_*') }}

  union distinct

  -- healthy blue operates the same way. rate cell / category value can change on a daily basis
  select
    patient.externalId as partnerMemberId,
    data.Enrollment_Start_Date as dateEffectiveFrom,
    data.Program_Category_Code as category,
    _table_suffix as fileDate
  from {{ source('healthy_blue_silver', 'member_*') }}
),

member_index_member_datasource_identifier as (

  select distinct
    memberId as patientId,
    externalId as partnerMemberId

  from {{ source('member_index', 'member_datasource_identifier') }}

  where deletedAt is null

),

tufts_latest_span as (
  select 
    partnerMemberId,
    max(dateEffectiveFrom) as maxFrom
  from tufts_member_daily
  group by 1
),

tufts_latest_record as (
  select 
    ls.partnerMemberId,
    max(maxFrom) as maxFrom,
    max(fileDate) as maxFileDate,
  from tufts_latest_span ls
  left join tufts_member_daily md 
    on ls.partnerMemberId = md.partnerMemberId 
      and ls.maxFrom = md.dateEffectiveFrom
  group by 1
),

tufts_latest_category_ranked as (

  select 
    mdi.patientId,
    md.category,
    rank() over(partition by patientId order by category) as rnk

  from tufts_latest_record lr

  inner join member_index_member_datasource_identifier mdi
    using (partnerMemberId)

  left join tufts_member_daily md
    on lr.partnerMemberId = md.partnerMemberId
       and lr.maxFrom = md.dateEffectiveFrom
       and lr.maxFileDate = md.fileDate

),

tufts_latest_category as (

  select * except (rnk)
  from tufts_latest_category_ranked
  where rnk = 1

),

result as (
  select
    m.patientId,
    coalesce(lc.category, c.category) as latestRatingCategory
  from src_member m
  left join member_index_member mim using(patientId)
  left join member_index_category c using(categoryId)
  left join tufts_latest_category lc using(patientId)
)

select * from result