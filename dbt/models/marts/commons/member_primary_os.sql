with os_rank as (

    select
        ct.*, 
        u.userName,
        u.userRole, 
        u.podName, 
        u.userEmail as primaryOsEmail, 
        RANK() OVER (PARTITION BY patientId ORDER BY createdAt DESC) AS rank

    from {{ source('commons', 'care_team') }} as ct

    left join {{ ref('user') }} as u
        on ct.userId = u.userId

    where ct.deletedAt IS NULL
        and u.userRole = 'Outreach_Specialist'

),

member_primary_os as (

    select
        userId, 
        patientId, 
        podName AS primaryOsPodName,
        createdAt as memberOsRelationCreatedAt, 
        userName as primaryOs, 
        primaryOsEmail

    from os_rank

    where rank = 1

)

select * from member_primary_os