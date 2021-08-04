
with latest as (

    select
        t.* except(rnk)

    from (
      select
          t.*,
          RANK() OVER (
            PARTITION BY patient.patientId
            ORDER BY insertedAt DESC, CAST(messageId AS INT64) DESC)
          as rnk

      from {{ source('medical', 'patient_encounters') }} t) t

    where rnk = 1

)

select * from latest
