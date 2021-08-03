with member_phones as (

  select
    memberId as patientId,
    phone as phoneNumber,
    phoneType,
    isPrimaryPhone,
    count(id) over(partition by memberId) as memberPhoneCount

  from {{ ref('abs_member_phones') }}

),

final as (

  select
    patientId,
    memberPhoneCount > 1 as hasMultipleNumbers,
    * except (patientId, memberPhoneCount)

  from member_phones

)

select * from final
