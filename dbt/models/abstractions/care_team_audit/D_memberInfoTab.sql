WITH patient_info AS (
  SELECT
    MAX(updatedAt),
    patientId,
    preferredName,
    maritalStatus,
    gender,
    transgender,
    language,
    CASE WHEN isWhite THEN "White" # separating out race and hispanic questions as they are separate and responses to both questions are required for chp audits
      WHEN isBlack THEN "Black"
      WHEN isAmericanIndianAlaskan THEN "American Indian Alaskan"
      WHEN isAsian THEN "Asian"
      WHEN isOtherRace THEN raceFreeText
      ELSE NULL
      END AS race,
    isHispanic,
    isMarginallyHoused,
    canReceiveCalls,
    preferredContactMethod,
    preferredContactTime,
    CASE WHEN hasUploadedPhoto THEN 'Yes'
      WHEN hasDeclinedPhotoUpload AND hasUploadedPhoto IS FALSE THEN 'Declined'
      ELSE 'No'
      END AS hasPhoto
  FROM
    {{ source('commons','patient_info') }}
    GROUP BY 
      patientId,
      needToKnow,
      updatedAt, #need to get max updatedAt date per patientId to get most recent view of needToKnow
      preferredName,
      maritalStatus,
      gender,
      transgender,
      language,
      race,
      isHispanic,
      isMarginallyHoused,
      canReceiveCalls,
      preferredContactMethod,
      preferredContactTime,
      hasPhoto
   ),

  member_commons_completion AS (
    SELECT
      patientId,
      hasHealthcareProxy,
      hasMolst,
      hasAdvanceDirectives
    FROM {{ ref('member_commons_completion') }}
    ),
    
    patient AS (
    SELECT
      Id,
      CoreIdentityVerifiedAt
    FROM {{ source('commons','patient') }}
    )

SELECT 
  pi.patientId,
  CASE WHEN p.CoreIdentityVerifiedAt IS NULL THEN false
    WHEN p.CoreIdentityVerifiedAt IS NOT NULL THEN true
    ELSE NULL
    END AS coreIdentityVerified,
  pi.preferredName,
  pi.maritalStatus,
  pi.gender,
  pi.transgender,
  pi.language,
  pi.race,
  pi.isHispanic,
  pi.isMarginallyHoused,
  pi.canReceiveCalls,
  pi.preferredContactMethod,
  pi.preferredContactTime,
  mcc.hasHealthcareProxy,
  mcc.hasMolst,
  mcc.hasAdvanceDirectives,
  pi.hasPhoto
FROM 
  patient_info pi
INNER JOIN
  member_commons_completion mcc
ON pi.patientId = mcc.patientId
INNER JOIN patient p
ON pi.patientId = p.Id