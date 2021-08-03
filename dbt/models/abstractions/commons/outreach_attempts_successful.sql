

{{
  config(
    materialized='ephemeral'
  ) 
}}


with outreach_attempts_successful AS (
  
    select distinct 
        CONCAT(
            patientId, 
            userId, 
            "OutreachIntake", 
            modality, 
            CAST(CAST(attemptedAt AS date) AS string)
            ) AS connectionKey,
    
        id,
        userId,
        patientId,
        createdAt,
        attemptedAt AS completedAt,
        CAST(attemptedAt AS date) AS completedAtDate,
        "Outreach/Intake" AS title,
        modality AS location

  from {{ source('commons', 'outreach_attempt') }} as oa
  
  where personReached = 'member'

)

select * from outreach_attempts_successful