SELECT 
    ct.patientId,
    ct.userId,
    ct.createdAt as careTeamMemberAssignedAt,
    ct.deletedAt as careTeamMemberAssignedUntil,
    u.userRole,
    u.userName,
    u.userEmail,
    u.userPhone,
    u.podName,
    ct.isPrimaryContact

FROM {{ source('commons', 'care_team') }} ct
LEFT JOIN {{ ref('user') }} u USING(userId)
WHERE u.userTerminationDate IS NULL