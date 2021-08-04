
select 
  memberId,
  safe_cast(dateOfBirth as date) as dateOfBirth,
  safe_cast(dateOfDemise as date) as dateOfDemise,
  sex

from {{ source('member_index', 'member_demographics') }}

where deletedAt is null
