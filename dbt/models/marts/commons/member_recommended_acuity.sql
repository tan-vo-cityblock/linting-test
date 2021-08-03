with member_recommended_acuity as (

    select
        patientId,
        acuity as recommendedMemberAcuityScore,
        createdAt as recommendedMemberAcuityCreatedAt,
        {{ convert_acuity_score_to_description(acuity_col = 'acuity') }} as recommendedMemberAcuityDescription

    from {{ source('commons', 'recommended_patient_acuity') }}

    where deletedAt is null

)

select * from member_recommended_acuity
