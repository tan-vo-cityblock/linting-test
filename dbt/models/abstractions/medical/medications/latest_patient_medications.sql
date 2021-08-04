SELECT t.* EXCEPT(rnk)
FROM (SELECT t.*,
             RANK() OVER (PARTITION BY patient.patientId
                                ORDER BY CAST(messageId AS INT64) DESC
                               ) as rnk
      FROM {{ source('medical', 'patient_medications') }} t
     ) t
WHERE rnk = 1