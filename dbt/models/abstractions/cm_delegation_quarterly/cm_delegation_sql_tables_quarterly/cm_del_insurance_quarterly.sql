SELECT DISTINCT
    mem.identifier.patientId,
    elig.detail.lineOfBusiness,
    elig.detail.subLineOfBusiness,
    current_date as date_run
FROM {{ source('emblem', 'Member') }} mem,
    unnest(eligibilities) as elig

WHERE mem.identifier.patientId is not null
    AND elig.date.from = (SELECT max(elig.date.from)
    FROM {{ source('emblem', 'Member') }} mem, unnest(eligibilities) as elig)

UNION ALL

SELECT DISTINCT
    mem.identifier.patientId,
    elig.detail.lineOfBusiness,
    elig.detail.subLineOfBusiness,
    current_date as date_run
FROM {{ source('cci', 'Member') }} mem ,
    unnest(eligibilities) as elig

WHERE mem.identifier.patientId is not null
    AND elig.date.from = (SELECT max(elig.date.from)
    FROM {{ source('cci', 'Member') }} mem, unnest(eligibilities) as elig)
