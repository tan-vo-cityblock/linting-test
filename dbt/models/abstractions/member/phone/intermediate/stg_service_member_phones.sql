{{- config(tags=["nightly"]) -}}

with ranked_member_phones as (

  select
    {{ dbt_utils.surrogate_key(['memberId', 'phone']) }} as id,
    'service' as source,
    memberId,
    phone,
    if(phoneType = 'main', 'unknown', phoneType) as phoneType,
    phoneType = 'main' as isPrimaryPhone,
    createdAt,

    {#- A handful of member phones appear with multiple records for a single `createdAt` value -#}
    rank() over(partition by memberId, phone order by createdAt desc, id) rnk

  from {{ ref('src_member_phone') }}
  where deletedAt is null

),

final as (

  select * except (rnk)
  from ranked_member_phones
  where rnk = 1

)

select * from final
