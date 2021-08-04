with mrt_claims_self_service as (

  select *,
    date_trunc(dateFrom, month) as monthFrom,
    extract(year from dateFrom) as yearFrom,

    lower(
      split(providerServicingName, ' ')[safe_offset(0)] || ' ' ||
      array_reverse(split(providerServicingName, ' '))[safe_offset(0)]
    ) as providerName

  from {{ ref('mrt_claims_self_service') }}

  where
    date_diff(current_date('America/New_York'), dateFrom, year) <= 3 and
    claimLineStatus in ('Paid', 'Encounter')

),

final as (

  select *,
    cast(4 - (extract(year from current_date) - yearFrom) as string) as yearSeq

  from mrt_claims_self_service

)

select * from final
