with member_acuity as (

    select
        patientId, 
        updatedById as updatedByUserId, 
        score,
        
        {{ convert_acuity_score_to_description() }} as memberAcuityDescription,

        extract(DATE FROM createdAt AT TIME ZONE "America/New_York") as memberAcuityDateEST,
        createdAt as memberAcuityCreatedAt,
        deletedAt AS memberAcuityDeletedAt
  
    from {{ source('commons', 'patient_acuity') }}

    where isRecommended = false

),

member_historical_acuity as (

    select
        row.*

    from (

        select
            ARRAY_AGG(t order by memberAcuityCreatedAt DESC LIMIT 1)[OFFSET(0)] as row

        from member_acuity as t

        group by patientId, memberAcuityDateEST

        )
)

select * from member_historical_acuity
