-- health home tracking report segment records file
-- author: nabig chaudhry

-- cte 1: obtaining health home enrollment and status info
with hh_status as (
  select distinct patientId,
    max(createdAt) as consentDate, 
    format_date("%m%d%Y",date(timestamp_trunc(createdAt, month))) as beginDate,
    case 
      when documentType = 'healthHomeInformationConsent' then 'in-person'
      when documentType = 'verbalHealthHomeInformationConsent' then 'verbal'
      end as typeOfConsent,
    uploadedById

  from {{ source('commons', 'patient_document') }}
  
  where documentType in ('healthHomeInformationConsent','verbalHealthHomeInformationConsent') and deletedAt is null
  group by createdAt, patientId, uploadedById, typeOfConsent
  
),

-- cte 2: obtaining name, date of birth, medicaid id, member id, and member status info
member_info as (
  select m.patientId,
    m.patientName as fullName,
    format_date('%m%d%Y', cast(m.dateOfbirth as date)) as dateOfBirth,
    m.medicaidId,
    i.gender,
    m.memberid

  from {{ ref('member') }} m
  inner join {{ ref('member_info') }} i
  on m.patientId = i.patientId

  
),

-- cte 3: obtaining member states and disenrollment info
member_states as (
  select patientId, 
    currentState,
    case
      when disenrolledAt is not null then format_date("%m%d%Y", date_sub(date_trunc(date_add(date(timestamp_trunc(disenrolledAt, month)),interval 1 month), month), interval 1 day))
      else repeat(' ', 8)
      end as endDate

    from {{ ref('member_states') }}

),

-- cte 4: obtaining health home consenter information
hh_consenter as (
  select userId, 
  userName as healthHomeConsenterName, 
  userRole as healthHomeConsenterRole

  from {{ ref('user') }} 

),

-- cte 5: joining together tables and formating fields for report
final as (
  -- nonrequired health home tracking report fields
  select distinct hs.patientId,
    mi.fullName,
    hs.typeOfConsent,
    hc.healthHomeConsenterName,
    hc.healthHomeConsenterRole,

  -- required health home tracking report fields
    'C' as recordType,
    mi.medicaidId,
    mi.dateOfBirth,
    case 
      when mi.gender = 'male' then 'M'
      when mi.gender = 'female' then 'F'
      else 'U' 
     end as gender,
    hs.beginDate,
    ms.endDate,
    'E' as outreachEnrollmentCode,
    '05541231' as healthHomeMmisId,
    '05541231' as careManagementAgencyMmisId,
    repeat(' ', 1) as directBillIndicator,
    'A' as adultOrChildServices,
    repeat(' ', 1) as tbd2,
    'R' as referralCode,
    repeat(' ', 2) as endPendReasonCode,
    format_date("%m%d%Y", cast(hs.consentDate as date)) as healthHomeConsentedAt,
    repeat(' ', 9) as nysid,
    repeat(' ', 40) as segmentEndDateComment,
    repeat(' ', 8) as pendStartDate,
    repeat(' ', 2) as pendReasonCode,
    repeat(' ', 40) as pendReasonCodeComment,
    'N' as endHealthHomeAssignment,
    mi.memberid

  -- cte joins
  from hh_status hs 
  inner join member_info mi
  on hs.patientId = mi.patientId
  inner join member_states ms
  on mi.patientId = ms.patientId
  inner join hh_consenter hc
  on hs.uploadedById = hc.userId 
  
)

-- final query
   select * from final