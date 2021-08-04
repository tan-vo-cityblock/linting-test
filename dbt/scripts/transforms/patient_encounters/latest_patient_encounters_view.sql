-- Partitions the patient_encounters_v2 table on encounter id and orders rows by messageId (DESC) to generate view
-- of most recent/current state of patient encounters per patient.

{{
  config(
    materialized = 'view'
  )
}}

SELECT rankedEncounters.* EXCEPT(rnk)
FROM
    (SELECT encounters.*,
            RANK() OVER (PARTITION BY encounter.identifiers.id
                         ORDER BY CAST(messageId AS INT64) DESC) AS rnk
     FROM {{ ref('patient_encounters') }} AS encounters) AS rankedEncounters
WHERE rnk = 1