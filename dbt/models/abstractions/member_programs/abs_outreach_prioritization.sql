
{{ config(tags = ["nightly"]) }}

with market_created_dates as (

  select distinct market, createdDate
  from {{ source('member_programs', 'src_outreach_prioritization') }}

),

market_deleted_dates as (

  select 
    market,
    createdDate,
    lead(createdDate) over(partition by market order by createdDate) as deletedDate
  from market_created_dates 

),

final as (

  select 
    sop.patientId,
    sop.rank,
    sop.isPriority,
    sop.createdDate,
    mdd.deletedDate,
    sop.market
  from {{ source('member_programs', 'src_outreach_prioritization') }} sop
  left join market_deleted_dates mdd
  using (market, createdDate)

)

select * from final
