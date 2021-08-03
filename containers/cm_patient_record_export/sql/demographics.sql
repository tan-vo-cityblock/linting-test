with info as (
  select patientId, 
         patientName,
         nmi as planPartnerId,
         productDescription as planInformation
  from `cityblock-analytics.mrt_commons.member` ),

--since member phone number is in Commons mirror tables, but not member index tables,
-- hence the member_phone_numbers CTE is a temp until member_index doesnt sync up completely with commons mirror.

member_phone_numbers as (
    select patientId,
           string_agg(phoneNumber, ' ;  ') as phoneNumbers
    from `cityblock-analytics.mrt_commons.member_phone_numbers`
    group by 1
),

demographics as (
  select patientId,
  dateofBirth,
  age,
  gender,
  raceEthnicity,
  transgender,
  language,
  education,
  employment,
  concat(ifnull(street1, ''),
    ifnull(street2, ''),
    ' ',ifnull(city,''),
    ' ',ifnull(state,''),
    ' ',ifnull(zip,''))
    as address
  from `cityblock-analytics.mrt_commons.member_info` ),
  
final as (
  select i.patientId as memberId,
  i.patientName as memberName,
  d.dateOfBirth,
  round(d.age, 2) as age,
  d.gender,
  d.raceEthnicity,
  mpn.phoneNumbers as contact,
  d.address,
  i.planPartnerId,
  i.planInformation,
  d.transgender as transgenderStatus,
  d.language as primaryLanguageSpoken,
  d.education as highestLevelOfEducation,
  d.employment as employmentStatus
  from info i
  inner join demographics d
  on i.patientId = d.patientId
  inner join member_phone_numbers mpn
  on i.patientId = mpn.patientId
  )
  
select * from final
