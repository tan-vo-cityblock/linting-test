
with member_address_city as (
    select
        patientId,
        --just replacing the characters they see for now because we do not want to get rid of all special characters
        regexp_replace(street1,'[#,():]','') as street1,
        city

    from `cityblock-analytics.mrt_commons.member_info`
),

partner_member_id as (
    select
        memberId,
        externalId,
        trim(SUBSTR(externalId, 2, 8)) medicaidId,
        trim(cast(mac.street1 as string)) street,
        trim(cast(mac.city as string)) city

    from `cbh-db-mirror-prod.member_index_mirror.member_datasource_identifier` mdi
    left join member_address_city mac
    on mdi.memberId = mac.patientId
    where mdi.current is true
),

billing_file as (
  select
    Member_ID as medicaidId,
    Add_Void_Indicator,
    Member_First_Name as firstName,
    Member_Last_Name as lastName,
    Member_Gender as gender,

    case
      when char_length(cast(Member_DOB as string)) = 8 then cast(Member_DOB as string)
      when char_length(cast(Member_DOB as string)) = 7 then concat('0', Member_DOB)
    end as dob,

    case
      when char_length(cast(Service_Date as string)) = 8 then cast(Service_Date as string)
      when char_length(cast(Service_Date as string)) = 7 then cast(concat('0', Service_Date) as string)
    end as serviceDate,

    Member_Zip_Code as zipcode,
    Rate_Amount as rateAmount,
    Rate_Code as rateCode,
    Last_Transaction_Date_Time as lastTransaction

  from `cityblock-data.src_health_home.health_home_billing`

),

billing_file_join_partner_member_id as (
    select
        *

    from billing_file
    left join partner_member_id
    using(medicaidId)
),

final as (
  select
    concat(medicaidId,serviceDate) as id,
    'C' as recordType,
    externalId as claimMemberId,
    firstName as claimMemberFirstName,
    '' as claimMemberMiddleIntial,
    lastName as claimMemberLastName,
    street as claimMemberAddress,
    city as claimMemberCity,
    'NY' as claimMemberState,
    zipcode as claimMemberZip,
    concat(substr(dob,0,1),substr(dob,2,3),substr(dob,5,7)) as claimMemberDob,
    gender as claimMemberGender,
    concat(medicaidId,serviceDate,substr(lastTransaction,0,2)) as claimAccountNumber,
    rateAmount as claimTotalChargeAmount,
    case when Add_Void_Indicator = 'V' then 8
      else 1 end as claimFrequencyCode,
    concat(substr(serviceDate,5,7),substr(serviceDate,0,1),substr(serviceDate,2,3),'-',substr(serviceDate,5,7),substr(serviceDate,0,1),substr(serviceDate,2,3)) as claimServiceDate,
    '' as claimAdmissionTypeCode,
    '' as claimAdmissionSourceCode,
    '01' as claimPatientStatusCode,
    concat(medicaidId,serviceDate,substr(lastTransaction,0,4)) as claimId,
    'Z7689' as claimDiagnosisCode,
    '' as claimDiagnosisCodeOther1,
    '' as claimDiagnosisCodeOther2,
    '' as claimDiagnosisCodeOther3,
    '' as claimDiagnosisCodeOther4,
    '' as claimDiagnosisCodeOther5,
    rateCode as claimRateCode,
    '' as claimCmaFirstName,
    '' as claimCmaMiddleIntial,
    'Unknown' as claimCmaLastName,
    '1912491770' as claimCmaNpi

    from billing_file_join_partner_member_id
    order by id
)

select * from final
