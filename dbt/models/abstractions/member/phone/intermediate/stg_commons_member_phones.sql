with member_phone_ids as (

  select
    patientId as memberId,
    phoneId

  from {{ source('commons', 'patient_phone') }}
  where deletedAt is null

),

phones as (

  select
    id as phoneId,

    {#- remove '+1' from beginning of string, leaving ten-digit phone -#}
    substr(phoneNumber, 3) as phone,

    type as phoneType,
    createdAt

  from {{ source('commons', 'phone') }}
  where deletedAt is null

),

member_primary_phones as (

  select
    patientId as memberId,
    primaryPhoneId as phoneId

  from {{ source('commons', 'patient_info') }}
  where primaryPhoneId is not null

),

member_phones as (

select
  {{ dbt_utils.surrogate_key(['mpi.memberId', 'p.phone']) }} as id,
  'commons' as source,
  mpi.memberId,
  p.phone,
  p.phoneType,
  mpp.phoneId is not null as isPrimaryPhone,
  p.createdAt

from phones p

inner join member_phone_ids mpi
using (phoneId)

left join member_primary_phones mpp
using (phoneId)

),

{#-
  Some member phones may be listed as primary and non-primary,
  and/or be listed as non-primary more than once
-#}

ranked_member_phones as (

  select *,
    rank() over(partition by id order by isPrimaryPhone desc, createdAt desc) as rnk

  from member_phones

),

final as (

  select * except (rnk)
  from ranked_member_phones
  where rnk = 1

)

select * from final
