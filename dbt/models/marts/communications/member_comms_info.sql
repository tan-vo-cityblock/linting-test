WITH members AS (
  SELECT
    patientId,
    firstName,
    middleName, 
    lastName,
    dateOfBirth,
    patientHomeMarketName,
    currentState in ("consented","enrolled","disenrolled_after_consent") as isPostConsent
  FROM {{ ref('member') }}
  -- Add any markets using Karuna here
  WHERE
  (patientHomeMarketName = "Massachusetts"
    AND currentState != "disenrolled" 
    AND currentState != "attributed")
  OR
  (patientHomeMarketName IN ("CareFirst DC", "North Carolina")
    AND currentState != "disenrolled")
),
phone_numbers_base AS (
  SELECT CONCAT(patientId, phoneNumber) as patientPhoneKey, 
  patientId, phoneNumber, phoneType, isPrimaryPhone,
  CASE 
    WHEN phoneType = 'home' THEN 1
    WHEN phoneType = 'mobile' THEN 2
    WHEN phoneType = 'work' THEN 3
    WHEN phoneType = 'other' THEN 4
    ELSE 5
  END as phoneTypeOrder
  FROM {{ ref('member_phone_numbers') }} 
  UNION ALL
  SELECT CONCAT(patientId, phone) as patientPhoneKey, 
  patientId, phone as phoneNumber, 'from tufts roster' as phoneType, 
  CAST(NULL as BOOLEAN) as isPrimaryPhone,
  10 as phoneTypeOrder
  FROM {{ source('tufts_phone_numbers', 'tufts_phone_numbers_supplemental_cleaned') }} -- To be removed once these are available in Commons
  -- Remove Tufts supplement numbers that are already in Commons
  WHERE CONCAT(patientId, phone) NOT in (
    SELECT CONCAT(patientId, phoneNumber) FROM {{ ref('member_phone_numbers') }} 
    )
),
phone_numbers AS (
  SELECT patientPhoneKey, patientId, phoneNumber, phoneType, isPrimaryPhone,
  RANK() OVER (PARTITION BY patientId 
    ORDER BY isPrimaryPhone DESC, phoneTypeOrder ASC,
    phoneNumber ASC) -- this is just to handle multiple supplemental Tufts numbers
  as phoneRank
  FROM phone_numbers_base
),
primary_phone_numbers AS ( 
  SELECT patientPhoneKey, patientId, phoneNumber, phoneType 
  FROM phone_numbers
  WHERE phoneRank = 1
),
secondary_phone_numbers AS (
  SELECT patientPhoneKey, patientId, phoneNumber, phoneType
  FROM phone_numbers
  WHERE phoneRank = 2
  -- Remove duplicate numbers in Commons (member has 2 records for same phone number, with 2 different phoneTypes)
  AND patientPhoneKey NOT in (
    SELECT patientPhoneKey FROM primary_phone_numbers
    )
),
tertiary_phone_numbers AS (
  SELECT patientPhoneKey, patientId, phoneNumber, phoneType
  FROM phone_numbers
  WHERE phoneRank = 3
  -- Remove duplicate numbers in Commons (see above CTE)
  AND patientPhoneKey NOT in (
    SELECT patientPhoneKey FROM primary_phone_numbers
    UNION ALL
    SELECT patientPhoneKey FROM secondary_phone_numbers
    )
),
contacts AS (
  SELECT
  patientId,
  pcon.firstName,
  pcon.lastName,
  CASE 
    WHEN pcon.relationToPatient = 'neighbor' THEN 'Friend'
    WHEN pcon.relationToPatient = 'roommate' THEN 'Friend'
    WHEN pcon.relationToPatient = 'friend' THEN 'Friend'
    WHEN pcon.relationToPatient = 'child' THEN 'Child'
    WHEN pcon.relationToPatient = 'parent' THEN 'Parent'
    WHEN pcon.relationToPatient = 'grandparent' THEN 'Grandparent'
    WHEN pcon.relationToPatient = 'grandchild' THEN 'Grandchild'
    WHEN pcon.relationToPatient = 'partner' THEN 'Partner'
    WHEN pcon.relationToPatient = 'spouse' THEN 'Spouse'
    WHEN pcon.relationToPatient = 'sibling' THEN 'Sibling'
    WHEN pcon.relationToPatient = 'other' THEN 'Other'
    ELSE pcon.relationToPatient
  END as relationToPatient,
  STRING(NULL) as gender,
  STRING(NULL) as primaryLanguage,
  pcon.isEmergencyContact,
  ph.phoneNumber, ph.type as phoneLabel,
  RANK() OVER(PARTITION BY pcon.patientId ORDER BY pcon.isEmergencyContact DESC, pcon.createdAt ASC) as contactRank
  FROM {{ source('commons', 'patient_contact') }} pcon
  LEFT JOIN {{ source('commons', 'patient_contact_phone') }} cph
  ON pcon.id = cph.patientContactId AND cph.deletedAt is NULL
  LEFT JOIN {{ source('commons', 'phone') }} ph
  ON cph.phoneId = ph.id AND ph.deletedAt IS NULL
),
primary_contact AS (
  SELECT * except(contactRank) FROM contacts
  WHERE contactRank = 1
),
secondary_contact AS (
  SELECT * except(contactRank) FROM contacts
  WHERE contactRank = 2
),
tertiary_contact AS (
  SELECT * except(contactRank) FROM contacts
  WHERE contactRank = 3
),
care_team AS (
  SELECT patientId, userId, isPrimaryContact, userRole, userEmail, careTeamMemberAssignedAt
  FROM {{ ref('member_current_care_team_all') }} ct
),
care_team_with_market as (
  SELECT care_team.*,
    members.patientHomeMarketName,
    members.isPostConsent
  FROM care_team 
  LEFT JOIN members 
  USING (patientId)
),
primary_care_team_dc AS (
  SELECT *,
  RANK() OVER (PARTITION BY patientId 
    ORDER BY 
    userRole = 'Community_Health_Partner' DESC,
    userRole = 'Lead_Community_Health_Partner' DESC,
    userRole = 'Member_Services_Advocate' DESC,
    careTeamMemberAssignedAt DESC) as primaryCareTeamRank
  FROM care_team_with_market
  WHERE userRole in ('Member_Services_Advocate', 'Community_Health_Partner', 'Lead_Community_Health_Partner') AND
    patientHomeMarketName = 'CareFirst DC'
),
primary_care_team_ma AS (
  SELECT *,
  RANK() OVER (PARTITION BY patientId 
    ORDER BY 
    isPrimaryContact DESC,
    userRole = 'Senior_Community_Health_Partner' DESC,
    userRole = 'Community_Health_Partner' DESC,
    userRole = 'Member_Services_Advocate' DESC,
    careTeamMemberAssignedAt DESC) as primaryCareTeamRank
  FROM care_team_with_market
  WHERE 
    (isPrimaryContact OR userRole in ('Senior_Community_Health_Partner', 'Community_Health_Partner', 'Member_Services_Advocate')) AND
    patientHomeMarketName = 'Massachusetts'
),
primary_care_team_nc AS (
  SELECT *,
  RANK() OVER (PARTITION BY patientId 
    ORDER BY 
    userRole = 'Community_Health_Partner' DESC,
    userRole = 'Care_Team_Lead' DESC,
    userRole = 'Care_Quality_Manager_RN' DESC,
    userRole = 'Nurse_Care_Manager' DESC,
    careTeamMemberAssignedAt DESC) as primaryCareTeamRank
  FROM care_team_with_market
  WHERE userRole in ('Nurse_Care_Manager', 'Community_Health_Partner', 'Care_Team_Lead', 'Care_Quality_Manager_RN') AND
    isPostConsent AND
    patientHomeMarketName = 'North Carolina'
),
primary_care_team AS (
  SELECT * FROM primary_care_team_dc
  UNION ALL
  SELECT * FROM primary_care_team_ma
  UNION ALL
  SELECT * FROM primary_care_team_nc
),
care_team_agg AS (
  SELECT care_team.patientId, 
  primary_care_team.userId as primaryCareTeamMemberId,
  ARRAY_AGG(care_team.userId) as careTeamMemberIds
  FROM care_team
  LEFT JOIN primary_care_team
  ON care_team.patientId = primary_care_team.patientId AND primary_care_team.primaryCareTeamRank = 1
  GROUP BY 1,2
),
final_cte AS (
SELECT
  member.patientId, member.firstName, member.middleName, member.lastName, 
  CONCAT(info.street1," ",info.street2) as address, 
  info.city, info.state, info.zip, 
  member.dateOfBirth, info.gender,
  pct.primaryCareTeamMemberId,
  ARRAY_TO_STRING(careTeamMemberIds, ", ") as careTeamMemberIds,
  pi.needToKnow, 

  primary_phone_numbers.phoneNumber as primaryPhoneNumber,
  primary_phone_numbers.phoneType as phone1Label,
  primary_phone_numbers.phoneNumber as phone1Number,
  -- If primary and secondary numbers are dupes (e.g. same phone number in Commons with 2 different types), 
  -- secondary would be NULL due to WHERE clause in secondary CTE, so use tertiary
  COALESCE(secondary_phone_numbers.phoneType, tertiary_phone_numbers.phoneType) as phone2Label,
  COALESCE(secondary_phone_numbers.phoneNumber, tertiary_phone_numbers.phoneNumber) as phone2Number,
  -- If tertiary number used in phone2 COALESCE() above, then phone3 fields should be NULL
  CASE 
    WHEN tertiary_phone_numbers.phoneNumber = COALESCE(secondary_phone_numbers.phoneNumber, tertiary_phone_numbers.phoneNumber)
    THEN STRING(NULL)
    ELSE tertiary_phone_numbers.phoneType 
  END as phone3Label,
  CASE 
    WHEN tertiary_phone_numbers.phoneNumber = COALESCE(secondary_phone_numbers.phoneNumber, tertiary_phone_numbers.phoneNumber)
    THEN STRING(NULL)
    ELSE tertiary_phone_numbers.phoneNumber 
  END as phone3Number,
  
  hasTextConsent, 

  primary_contact.firstName as secondaryContact1FirstName,
  primary_contact.lastName as secondaryContact1LastName,
  primary_contact.relationToPatient as secondaryContact1Relationship,
  primary_contact.isEmergencyContact as secondaryContact1IsEmergencyContact,
  primary_contact.phoneLabel as secondaryContact1Phone1Label,
  primary_contact.phoneNumber as secondaryContact1Phone1Number, 

  secondary_contact.firstName as secondaryContact2FirstName,
  secondary_contact.lastName as secondaryContact2LastName,
  secondary_contact.relationToPatient as secondaryContact2Relationship,
  secondary_contact.isEmergencyContact as secondaryContact2IsEmergencyContact,
  secondary_contact.phoneLabel as secondaryContact2Phone1Label,
  secondary_contact.phoneNumber as secondaryContact2Phone1Number, 

  tertiary_contact.firstName as secondaryContact3FirstName,
  tertiary_contact.lastName as secondaryContact3LastName,
  tertiary_contact.relationToPatient as secondaryContact3Relationship,
  tertiary_contact.isEmergencyContact as secondaryContact3IsEmergencyContact,
  tertiary_contact.phoneLabel as secondaryContact3Phone1Label,
  tertiary_contact.phoneNumber as secondaryContact3Phone1Number,

  STRING(NULL) as dummyColumnString,
  CAST(NULL AS BOOLEAN) as dummyColumnBool,

  patientHomeMarketName
  FROM members member
  LEFT JOIN {{ ref('member_info') }} info
  ON info.patientId = member.patientId
  LEFT JOIN {{ source('commons', 'patient_info') }} pi
  ON pi.patientId = member.patientId
  LEFT JOIN care_team_agg pct
  ON pct.patientId = member.patientId
  LEFT JOIN primary_phone_numbers 
  ON primary_phone_numbers.patientId = member.patientId
  LEFT JOIN secondary_phone_numbers
  ON secondary_phone_numbers.patientId = member.patientId
  LEFT JOIN tertiary_phone_numbers
  ON tertiary_phone_numbers.patientId = member.patientId
  LEFT JOIN primary_contact 
  ON primary_contact.patientId = member.patientId
  LEFT JOIN secondary_contact
  ON secondary_contact.patientId = member.patientId
  LEFT JOIN tertiary_contact
  ON tertiary_contact.patientId = member.patientId
)

SELECT 
  patientId, firstName, middleName, lastName, address, city, state, zip as zipCode, dateOfBirth as dob, 
  CASE WHEN gender IS NULL THEN 'unknown' ELSE gender END as sex, 
  dummyColumnString as primaryLanguage,
  primaryCareTeamMemberId,
  careTeamMemberIds,
  needToKnow as patientNotes,
  primaryPhoneNumber, 
  phone1Label, phone1Number, dummyColumnString as phone1Notes, dummyColumnBool as phone1ConsentGiven,
  phone2Label, phone2Number, dummyColumnString as phone2notes, dummyColumnBool as phone2ConsentGiven,
  phone3Label, phone3Number, dummyColumnString as phone3notes, dummyColumnBool as phone3ConsentGiven,
  secondaryContact1FirstName, dummyColumnString as secondaryContact1MiddleName, secondaryContact1LastName,
  secondaryContact1Relationship, dummyColumnString as secondaryContact1Gender, dummyColumnString as secondaryContact1PrimaryLanguage,
  secondaryContact1IsEmergencyContact, secondaryContact1Phone1Number as secondaryContact1PrimaryPhoneNumber, 
  secondaryContact1Phone1Label, secondaryContact1Phone1Number, 
  dummyColumnString as secondaryContact1Phone1Notes, dummyColumnBool as secondaryContact1Phone1ConsentGiven,
  secondaryContact2FirstName, dummyColumnString as secondaryContact2MiddleName, secondaryContact2LastName,
  secondaryContact2Relationship, dummyColumnString as secondaryContact2Gender, dummyColumnString as secondaryContact2PrimaryLanguage,
  secondaryContact2IsEmergencyContact, secondaryContact2Phone1Number as secondaryContact2PrimaryPhoneNumber, 
  secondaryContact2Phone1Label, secondaryContact2Phone1Number,  
  dummyColumnString as secondaryContact2Phone1Notes, dummyColumnBool as secondaryContact2Phone1ConsentGiven,
  secondaryContact3FirstName, dummyColumnString as secondaryContact3MiddleName, secondaryContact3LastName,
  secondaryContact3Relationship, dummyColumnString as secondaryContact3Gender, dummyColumnString as secondaryContact3PrimaryLanguage,
  secondaryContact3IsEmergencyContact, secondaryContact3Phone1Number as secondaryContact3PrimaryPhoneNumber, 
  secondaryContact3Phone1Label, secondaryContact3Phone1Number,  
  dummyColumnString as secondaryContact3Phone1Notes, dummyColumnBool as secondaryContact3Phone1ConsentGiven,
  patientHomeMarketName
from final_cte
