WITH consent AS (
  SELECT 
    isFullConsentSigned,
    patientId
    FROM  {{ source('commons', 'computed_patient_status') }}
    WHERE deletedAt is NULL) 

SELECT
  a.patientId AS patientId,
  a.currentMemberAcuityScore AS currentMemberAcuityScore,
  pi.needToKnow AS needToKnow,
  MAX(pi.updatedAt) as ntkLastUpdatedAt,
  c.isFullConsentSigned AS consentCompleted
FROM {{ ref('member_current_acuity') }} a
INNER JOIN {{ source('commons', 'patient_info') }} pi
ON a.patientId = pi.patientId
INNER JOIN consent c
ON a.patientId = c.patientId
GROUP BY
  a.patientId,
  a.currentMemberAcuityScore,
  pi.needToKnow,
  c.isFullConsentSigned