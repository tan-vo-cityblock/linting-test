  

{{
  config(
    materialized='ephemeral'
  ) 
}}


with outreach_attempt_rank as (

    select 
        *,
        RANK() OVER (PARTITION BY connectionKey ORDER BY completedAt ASC) AS outreachAttemptRank
    
    from {{ ref('outreach_attempts_successful') }}

)

select * from outreach_attempt_rank