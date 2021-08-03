
select distinct
  patientId,
  eligDate as monthFrom,
  extract(year from eligDate) as yearFrom

from {{ ref('master_member_v1') }}

where 
  date_diff(current_date('America/New_York'), eligDate, year) <= 3 and
  isInLatestPayerFile = 1 and
  memberId is not null and
  isOutOfAreaOriginally != 1 and
  isLatestVersion = 1

union distinct

select 
  patientId,
  eligDate as monthFrom,
  extract(year from eligDate) as yearFrom 
from {{ ref('master_member_mass_health') }}
where 
  date_diff(current_date('America/New_York'), eligDate, year) <= 3 

union distinct

select 
  patientId,
  eligDate as monthFrom,
  extract(year from eligDate) as yearFrom 
from {{ ref('master_member_dc_historic') }}
where 
  date_diff(current_date('America/New_York'), eligDate, year) <= 3 