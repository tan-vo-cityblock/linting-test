{{ config(materialized = 'table') }}

with member_authorization_ids as (

  select
    {{ dbt_utils.surrogate_key(
        ['patientId', 'authorizationId', 'isConcurrentInpatientAuthorization']
      ) 
    }} as id,

    id as authorizationVersionId,
    * except (id)

  from {{ ref('abs_prior_auth_emblem_cci_version_received_dates') }}

),

row_numbers as (

  select *,
    row_number() over(partition by id order by receivedDate desc) as rowNumber
    
  from member_authorization_ids

),

latest_version_dates as (

  select * except (rowNumber)
  from row_numbers
  where rowNumber = 1

)

select * from latest_version_dates
