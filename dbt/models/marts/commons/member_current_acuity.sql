with member_current_acuity as (

    select 
        patientId, 
        updatedById as updatedByUserId, 
        score as currentMemberAcuityScore, 
        createdAt as memberAcuityCreatedAt,
        {{ convert_acuity_score_to_description() }} as currentMemberAcuityDescription,        

        CASE 
            WHEN score=5 THEN 6
            WHEN score=4 THEN 6
            WHEN score=3 THEN 4
            WHEN score=2 THEN 2
            WHEN score=1 THEN 1
            ELSE 0
        END AS targetMonthlyTendsCurrentAcuity 
  
    from {{ source('commons', 'patient_acuity') }} 

    where deletedAt is null and isRecommended = false

)

select * from member_current_acuity
order by memberAcuityCreatedAt desc
