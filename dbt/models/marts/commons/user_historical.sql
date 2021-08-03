with pods_at_creation as (

  select 
    us.id,
    us.dbt_valid_from,
    min(cpm.createdAt) as earliestPodCreatedAt
  from {{ source('snapshots', 'user_snapshot' ) }} as us
  left join {{ source('commons', 'clinic_pod_member' )}} as cpm
  on 
    us.id = cpm.userId and
    (cpm.deletedAt > us.dbt_valid_from or cpm.deletedAt is null)
  group by us.id, us.dbt_valid_from

), 

user_historical as (

    select
        us.id as userId,
        concat(us.firstName," ",us.lastName) as userName,
        us.userRole,
        us.email as userEmail,
        cp.name as podNameAtUserUpdate,
        u.userHomeClinicName as currentClinicName,
        u.userHomeMarketName as currentMarketName,
        extract(date from us.createdAt) AS userCreationDate,
        us.dbt_valid_from as createdAt, 
        us.dbt_valid_to as deletedAt

    from {{ source('snapshots', 'user_snapshot' ) }} as us

    -- in a future state when users start moving markets and clinics we will need to join these as SCD 2 also
    -- for now we're only handling pod changes

    left join pods_at_creation pac
    using (id, dbt_valid_from)

    left join {{ source('commons', 'clinic_pod_member' )}} as cpm
    on 
      pac.id = cpm.userId and
      pac.earliestPodCreatedAt = cpm.createdAt  

    left join {{ source('commons', 'clinic_pod' )}} as cp
        on cpm.clinicPodId = cp.id

    left join {{ ref('user') }} as u
        on us.id = u.userId

    where 
        us.lastLoginAt is not null

)

select * from user_historical