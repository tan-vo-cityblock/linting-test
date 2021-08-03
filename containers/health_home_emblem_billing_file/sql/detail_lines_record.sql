with billing_file as (
  select
    Member_ID as medicaidId,
    Member_First_Name as firstName,
    Member_Last_Name as lastName,
    Member_Gender as gender,
    
    case 
      when char_length(cast(Member_DOB as string)) = 8 then cast(Member_DOB as string)
      when char_length(cast(Member_DOB as string)) = 7 then concat('0',Member_DOB)
    end as dob,
      
    case 
      when char_length(cast(Service_Date as string)) = 8 then cast(Service_Date as string)
      when char_length(cast(Service_Date as string)) = 7 then concat('0',Service_Date)
    end as serviceDate,
      
    Member_Zip_Code as zipcode,
    Rate_Amount as rateAmount,
    Rate_Code as rateCode,
    Last_Transaction_Date_Time as lastTransaction
  
  from `cityblock-data.src_health_home.health_home_billing`

),

final as (
  select
    concat(medicaidId,serviceDate) as id,
    'L' as recordType,
    '0500' as lineRevenueCode,
    'G9005' as lineProcedureCode,
    rateAmount as lineChargeAmount,
    case when rateCode = 1873 then 'U1'
    when ratecode = 1874 then 'U2'
    else null end as lineModifierCode1,
    '' as lineModifierCode2,
    '' as lineModifierCode3,
    '' as lineModifierCode4,
    '' as lineModifierCode5,
    '1' as lineUnitCount

    from billing_file
    order by id
    
)

select * from final