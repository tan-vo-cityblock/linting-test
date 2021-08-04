
{{ config(materialized = 'table') }}

with ranked_authorizations as (

  select *,
    rank() over(partition by authorizationNumber order by receivedDate desc) as rnk
    
  from {{ ref('abs_prior_auth_tufts_all') }}

),

latest_authorizations as (

  select * except (rnk)
  
  from ranked_authorizations  
  
  where rnk = 1

)

select * from latest_authorizations
