with all_member_phones as (

  select * from {{ ref('stg_commons_member_phones') }}
  union all
  select * from {{ ref('stg_service_member_phones') }}

),

{#- Identify single record per member phone, prioritizing Commons source -#}

ranked_member_phones as (

  select *,
    rank() over(partition by id order by source) as rnk

  from all_member_phones

),

final as (

  select * except (rnk)
  from ranked_member_phones
  where rnk = 1

)

select * from final
