WITH 
    molstDoc AS 
    (SELECT 
        patientId, 
        createdAt 
    FROM 
        {{ source('commons','patient_document') }}
    WHERE documentType in ('molst', 'molstForDisabilities')),

    molstQuestion AS 
    (SELECT 
        patientId, 
        hasMolst 
    FROM 
        {{ source('commons','patient_info') }} )

SELECT 
  d.patientId,
  q.hasMolst,
  d.createdAt
FROM 
  molstQuestion q
LEFT JOIN
  molstDoc d
ON q.patientId = d.patientId
