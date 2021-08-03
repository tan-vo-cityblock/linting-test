with elation_user_latest as (

  select * replace(regexp_replace(email, r"\+.*@", "@") as email),
    first_name || ' ' || last_name as fullName

  from {{ source('elation', 'user_latest') }}

),

commons_user as (

  select
    id as commonsUserId,
    email

  from {{ source('commons', 'user') }}

),

final as (

  select e.*,
    c.commonsUserId

  from elation_user_latest e

  left join commons_user c
  using (email)

)

select * from final
