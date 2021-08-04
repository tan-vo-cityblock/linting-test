with pod_assignments as (

  select
    userId,
    clinicPodId,
    row_number() over(partition by userId order by createdAt desc) as rowNum
  from {{ source('commons', 'clinic_pod_member') }}
  where deletedAt is null

),

latest_pod_assignments as (

  select userId, clinicPodId
  from pod_assignments
  where rowNum = 1

),

elation_user_id as (
  select 
    id,
    email
  from {{ source('elation', 'user_latest') }}
),

user as (

    select
        u.id                               as userId,
        concat(u.firstName," ",u.lastName) as userName,
        u.userRole,
        u.email                            as userEmail,
        u.phone                            as userPhone,
        u.npi                              as userNpi,
        cp.name                            as podName,
        c.name                             as userHomeClinicName,
        m.name                             as userHomeMarketName,
        extract(date from u.createdAt)     as userCreationDate,
        u.terminatedAt                     as userTerminationDate,
        e.id                               as elationUserId

    from {{ source('commons', 'user' )}} as u

    left join latest_pod_assignments lpa
      on u.id = lpa.userId

    left join {{ source('commons', 'clinic_pod' )}} as cp
      on lpa.clinicPodId = cp.id

    left join {{ source('commons', 'market' )}} as m
      on m.id = u.homeMarketId

    left join {{ source('commons', 'clinic') }} as c
      on c.id = u.homeClinicId

    left join elation_user_id as e
      using(email)

    where 
      cp.deletedAt is null and 
      u.deletedAt is null and 
      m.deletedAt is null and 
      u.lastLoginAt is not null

    order by userHomeClinicName, userRole

)

select * from user
