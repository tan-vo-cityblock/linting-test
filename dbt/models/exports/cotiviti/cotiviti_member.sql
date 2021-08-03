with member_population_states as (

  select distinct
    patientId,

    case when lineOfBusinessGrouped like 'Medicaid%' then 'Medicaid'
         when lineOfBusinessGrouped in ('DSNP', 'Duals') then 'Dual'
         when lineOfBusinessGrouped like 'Medicare%' then 'Medicare'
         when lineOfBusinessGrouped = 'Commercial' then 'Commercial'
         else null
    end as population,

    eligDate
  
  from {{ ref('master_member_v1') }}
  ),
  
  -- Use member service as primary source of truth for member demographics data
  member_demographics as (

    select distinct
      memberId as patientId,
      dateOfBirth,
      case
        when sex = 'female' then 'F'
        when sex = 'male' then 'M'
        else 'U'
      end as sex

    from {{ ref('src_member_demographics') }}
  ),

  -- In case a member is missing from member service, use member info as secondary source of truth
  member_info as (

    select
      patientId,
      dateOfBirth,
      case
        when gender = 'female' and transgender = 'no' then 'F'
        when gender = 'male' and transgender = 'yes' then 'F'
        when gender = 'male' and transgender = 'no' then 'M'
        when gender = 'female' and transgender = 'yes' then 'M'
        else 'U'
      end as sex

    from {{ ref('member_info') }}
  ),

  member_market_delivery_model as (

    select
      patientId,
      patientHomeMarketName as currentHomeMarketName,
      regexp_contains(patientHomeClinicName, '[Vv]irtual') as currentIsVirtual
    from {{ ref('abs_commons_member_market_clinic') }}
  ),

  member_state as (

    select
      patientId,
      currentState as currentEnrollmentStatus
    from {{ ref('member_states') }}

  ),
  
  by_month_with_dupes as (

    select
      mps.patientId,
      -- Treat member service's demographics table as primary source of truth for date of birth
      coalesce(md.dateOfBirth, mi.dateOfBirth) as dateOfBirth,
      coalesce(md.sex, mi.sex) as sex,
      mmdm.currentHomeMarketName,
      mmdm.currentIsVirtual,
      ms.currentEnrollmentStatus,
      mps.population,
      case when mps.population = 'Commercial' then 1 else 0 end as commercialFlag,
      case when mps.population in ('Medicare', 'Dual') then 1 else 0 end as medicareFlag,
      case when mps.population in ('Medicaid', 'Dual') then 1 else 0 end as medicaidFlag,
      mps.eligDate
    from member_population_states mps
    left join member_demographics md using (patientId)
    left join member_info mi using (patientId)
    left join member_market_delivery_model mmdm using (patientId)
    left join member_state ms using (patientId)
    order by 1, 11, 7
  ),

    by_month as (
  
    select
      patientId,
      dateOfBirth,
      sex,
      currentHomeMarketName,
      currentIsVirtual,
      currentEnrollmentStatus,
      max(commercialFlag) as commercialFlag,
      max(medicareFlag) as medicareFlag,
      max(medicaidFlag) as medicaidFlag,
      string_agg(distinct population, ', ' order by population) as population,
      eligDate,
      concat(patientId, coalesce(string_agg(distinct population, ', ' order by population), 'null')) as stateHash
    from by_month_with_dupes
    group by 1, 2, 3, 4, 5, 6, 11
  ),

  by_month_with_state_changes as (

    select
      *,
      -- Flag rows that are either the first or the last row of a given state
      coalesce(lag(stateHash) over (partition by patientId order by eligDate) != stateHash, True) as isStateStart,
      coalesce(lead(stateHash) over (partition by patientId order by eligDate) != stateHash, True) as isStateEnd
    from by_month
  ),
  
  by_month_with_state_numbers as (
  
    select
      *,
      -- Keep a running count of state changes to number unique states
      countif(isStateStart) over (partition by patientId order by eligDate) as stateNumber
    from by_month_with_state_changes
  ),

  final as (

    select
      first.* except (eligDate, stateHash, isStateStart, isStateEnd, stateNumber),
      first.eligDate as validFrom,
      -- Ensure there are no gaps by taking the last dat of the month as the valid-to-date, since eligDate is always the first day of the month
      date_sub(date_trunc(date_add(last.eligDate, interval 1 month),  month), interval 1 day) as validTo
    from by_month_with_state_numbers first
    left join by_month_with_state_numbers last
      on first.patientId = last.patientId
         and first.stateHash = last.stateHash
         and first.stateNumber = last.StateNumber
    where first.isStateStart
         and last.isStateEnd
  )

select * from final
order by patientId, validFrom
