--CTEs for HH, HARP, Medicaid End dates

with max_shard as (
  SELECT max(_table_suffix) as maxShard
  from {{ source('hh_harp', 'Health_Home_Status_*') }} 
  ),
  
base1 as (
  SELECT * 
  from {{ source('hh_harp', 'Health_Home_Status_*') }}  aa
  inner join max_shard ms 
    on aa._TABLE_SUFFIX = ms.maxShard
  where aa.Member_ID is not null
),

src_member_insurance as (

  select
    externalId as member_id,
    memberId as patientId
  
  from {{ ref('src_member_insurance') }}

),

base as (
  SELECT  
    Member_ID , First_Name , Last_Name,  
    Medicaid_Eligibility_End_Date as Medicaid_end , 
    Medicaid_Coverage_Code , 
    MCP_MMIS_Provider_ID , 
    Enrolled_HH_MMIS_Provider_ID , 
    segment_type,
    Medicaid_Recipient_Exemption_Code_1 as RE_1, 
    Medicaid_Recipient_Exemption_Code_2 as RE_2, 
    Medicaid_Recipient_Exemption_Code_3 as RE_3, 
    Medicaid_Recipient_Exemption_Code_4 as RE_4, 
    Medicaid_Recipient_Exemption_Code_5 as RE_5  
  FROM base1  --null memberids(mediciad_ids) are already filtered out.
),

Harp_codes as ( 
  select *
  from {{ source('hh_harp', 'HARP_codes') }}  
),

re_codes as ( 
  select *
  from {{ source('hh_harp', 'Incompatible_RE_Codes') }}  
),

--filtering out non medicaid ELIGIBLE and non emblem ELIGIBLE
emblem_ELIGIBLE as (
    select 
      base.*
    from base
    where 1=1
      --filtering out any incompatible medicaid coverage codes
     -- and base.medicaid_coverage_code not in    
      and base.MCP_MMIS_provider_id  in
        (00313979,  --HLTH INSURANCE PLAN OF GTR NY
        01131584,   --HIP WESTCHESTER
        01202822,   --HIP NASSAU COUNTY
        04082293    --HEALTH INSURANCE PLAN OF GREATER NE
        )
),

Medicaid_ELIGIBLE as (
    select 
      emblem_ELIGIBLE.*  
    from emblem_ELIGIBLE 
    where 1=1
    --filtering out any non emblem ELIGIBLE members 
    and coalesce(cast(medicaid_coverage_code as string), '99') not in ( select * from {{ source('hh_harp', 'Incompatible_Medicaid_Coverage_Codes') }} ) 
),
    
HARP as (
  select 
    medicaid_ELIGIBLE.*,
    case 
      when RE_1 in    (select harp_code from harp_codes where status = 'Enrolled') then 'ENROLLED'
      when RE_2 in    (select harp_code from harp_codes where status = 'Enrolled') then 'ENROLLED'
      when RE_3 in    (select harp_code from harp_codes where status = 'Enrolled') then 'ENROLLED'
      when RE_4 in    (select harp_code from harp_codes where status = 'Enrolled') then 'ENROLLED'
      when RE_5 in    (select harp_code from harp_codes where status = 'Enrolled') then 'ENROLLED'
      
      when RE_1 in    (select harp_code from harp_codes where status = 'Eligible') then 'ELIGIBLE'
      when RE_2 in    (select harp_code from harp_codes where status = 'Eligible') then 'ELIGIBLE'
      when RE_3 in    (select harp_code from harp_codes where status = 'Eligible') then 'ELIGIBLE'
      when RE_4 in    (select harp_code from harp_codes where status = 'Eligible') then 'ELIGIBLE'
      when RE_5 in    (select harp_code from harp_codes where status = 'Eligible') then 'ELIGIBLE'
      
      else 'INELIGIBLE' END as HARP_status
  from medicaid_ELIGIBLE
),

HH as (
  select 
    medicaid_ELIGIBLE.*,
    case    
      when RE_1 in    (select incompatible_codes from re_codes) then 'INELIGIBLE'
      when RE_2 in    (select incompatible_codes from re_codes) then 'INELIGIBLE'
      when RE_3 in    (select incompatible_codes from re_codes) then 'INELIGIBLE'
      when RE_4 in    (select incompatible_codes from re_codes) then 'INELIGIBLE'
      when RE_5 in    (select incompatible_codes from re_codes) then 'INELIGIBLE'      
      when Enrolled_HH_MMIS_Provider_ID in (05541231) then 'CBH ENROLLED'  --Toyin's Provider Id     
      when Enrolled_HH_MMIS_Provider_ID is not null then 'OTHER ENROLLED'  --any non null provider that remains
      else 'ELIGIBLE' end as HH_status
    from medicaid_ELIGIBLE
),

final as (
    select 
      medicaid_ELIGIBLE.*,
      case 
      when RE_1 in    (select harp_code from harp_codes where status = 'Enrolled') then 'ENROLLED'
      when RE_2 in    (select harp_code from harp_codes where status = 'Enrolled') then 'ENROLLED'
      when RE_3 in    (select harp_code from harp_codes where status = 'Enrolled') then 'ENROLLED'
      when RE_4 in    (select harp_code from harp_codes where status = 'Enrolled') then 'ENROLLED'
      when RE_5 in    (select harp_code from harp_codes where status = 'Enrolled') then 'ENROLLED'

      when RE_1 in    (select harp_code from harp_codes where status = 'Eligible') then 'ELIGIBLE'
      when RE_2 in    (select harp_code from harp_codes where status = 'Eligible') then 'ELIGIBLE'
      when RE_3 in    (select harp_code from harp_codes where status = 'Eligible') then 'ELIGIBLE'
      when RE_4 in    (select harp_code from harp_codes where status = 'Eligible') then 'ELIGIBLE'
      when RE_5 in    (select harp_code from harp_codes where status = 'Eligible') then 'ELIGIBLE'
      
      else 'INELIGIBLE' END as HARP_status,
      
      case
        when RE_1 in    (select incompatible_codes from re_codes) then 'INELIGIBLE'
        when RE_2 in    (select incompatible_codes from re_codes) then 'INELIGIBLE'
        when RE_3 in    (select incompatible_codes from re_codes) then 'INELIGIBLE'
        when RE_4 in    (select incompatible_codes from re_codes) then 'INELIGIBLE'
        when RE_5 in    (select incompatible_codes from re_codes) then 'INELIGIBLE'
        when Enrolled_HH_MMIS_Provider_ID in (05541231) then 'CBH ENROLLED'  --Toyin's Provider Id
        when Enrolled_HH_MMIS_Provider_ID is not null then 'OTHER ENROLLED'  --any non null provider
        else 'ELIGIBLE' end as HH_status
    from medicaid_ELIGIBLE
)

select distinct
  aa.member_id as medicaid_id,
  smi.patientId,
  first_name,
  Last_Name,
  cast(
    concat( substr(lpad(cast(Medicaid_end as string),8,'0'),5,4), '-',
          substr(lpad(cast(Medicaid_end as string),8,'0'),0,2), '-',
          substr(lpad(cast(Medicaid_end as string),8,'0'),3,2)
         ) 
       as date) as medicaidEndDate,
  harp_status as harpStatus,
  HH_status as healthHomeStatus,
  current_date() as reportDate
from final aa

left join src_member_insurance smi
using (member_id)
