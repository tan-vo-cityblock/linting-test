
-- creating a view of member_index that harkens back to the patient_index_cache and is more usable for DSA

{{ config(materialized='table') }}

with base as (
    
    select 
        m.id as patientId, 
        m.cbhId,
        mdi.current as active,
        m.deletedAt,
        d.name as memberIdSource,
        --  data source id, all external ids are stored in the same column, including partner ids and ehr ids
        mdi.externalId as memberId,
        --  partner; changing case to match what we used to have in PIC
        p.name as partnerName,
        p.id as partnerId, 
        -- cohortName, except tufts that doesn't fit the cohort model, so imputing placeholder value in case there is nothing in the member index    
        case 
          when p.name = 'tufts' and c.name is null        
            then 'Tufts Cohort Unknown'
          else c.name end as cohortName,
        -- tufts daily adds, use individual member golive
        -- per jac/tina 3/4/20 this should not be the commons createdAt but instead the shardDate from the earliest daily add/drop file; don't have access to those yet will replace later; for now proxy is good enough
        case when p.name = 'tufts' and c.name is null 
              then cast(m.createdAt as date)
              else c.goLiveDate end as cohortGoLiveDate, 
        -- manually adding revenue golive dates per actuary 11/13/19, before this is available in the member index itself
        cast(
         (case
            -- emblem
            -- when c.name = 'Emblem Cohort 1' then '2018-07-01'
            -- when c.name = 'Emblem Cohort 2' then '2018-09-01'
            -- when c.name in ('Emblem Cohort 3', 'Emblem Cohort 3a') then '2018-12-01'
            -- when c.name = 'Emblem Cohort 3b' then '2019-02-01'
            -- when c.name = 'Emblem Cohort 3c' then '2019-05-01'
            -- when c.name = 'Emblem Cohort 3c' then '2019-05-01'
            -- when c.name = 'Emblem Cohort 4a' then '2019-08-01'
            when c.name = 'Emblem Cohort 4b' then '2019-09-01'
            when c.name = 'Emblem Cohort 5a' then '2019-11-01'
            -- when c.name = 'Emblem Cohort 6a' then '2020-05-01'
            -- when c.name = 'Emblem Cohort 7' then '2020-07-01'
            -- when c.name = 'Emblem Medicaid Digital Cohort 1' then '2020-09-01'
            -- when c.name = 'Emblem Cohort 8' then '2021-01-01'
            -- cci
            -- when c.name = 'ConnectiCare Cohort 1a' then '2019-03-01'
            -- when c.name = 'ConnectiCare Cohort 1b' then '2019-06-01'
            -- when c.name = 'ConnectiCare Cohort 1c' then '2019-07-01'
            -- when c.name = 'ConnectiCare Cohort 1d' then '2020-01-01'
            -- when c.name = 'ConnectiCare Cohort 2' then '2020-06-01'
            -- tufts cohorts
            -- when c.name = 'Tufts Cohort 1' then '2020-03-01'
            -- when c.name = 'Tufts Cohort 2' then '2020-04-01'
            -- when c.name = 'Tufts Cohort 3' then '2020-10-01'     
            when c.name = 'Tufts Cohort 5' then '2021-08-01'           
            when c.name = 'Tufts Cohort 6' then '2021-08-01'           
            -- tufts daily adds, use individual member golive; 1st of that month (per jac/tina 3/4/20)
            -- per jac/tina 3/4/20 this should not be the commons createdAt but instead the shardDate from the earliest daily add/drop file; don't have access to those yet will replace later; for now proxy is good enough
            when p.name = 'tufts' and c.name is null 
              then date_trunc(cast(m.createdAt as date), month) 
            -- carefirst cohorts
            when c.name = 'CareFirst Cohort 1' then '2020-10-01'
            -- carefirst daily adds
            when p.name = 'carefirst' and c.name is null 
              then date_trunc(cast(m.createdAt as date), month)
            -- cardinal
            when c.name = 'Cardinal Cohort 1' then '2021-06-01'
            -- healthy blue
            when c.name = 'Healthy Blue 1' then '2021-07-01'
            when p.name = 'Healthy Blue' and c.name is null 
              then date_trunc(cast(m.createdAt as date), month) 
            else c.revenueGoLiveDate end) as date) as revenueGoLiveDate,
        c.id as cohortId,
        ca.name as category,
        m.mrnId
    from {{ source('member_index', 'member') }} m
    left outer join {{ source('member_index', 'member_datasource_identifier') }} mdi 
      on m.id = mdi.memberId
    left outer join {{ source('member_index', 'cohort') }} c 
      on m.cohortId = c.id
    left outer join {{ source('member_index', 'partner') }} p 
      on m.partnerId = p.id
    left outer join {{ source('member_index', 'datasource') }} d 
      on mdi.dataSourceId = d.id
    left outer join {{ source('member_index', 'category') }} ca 
      on m.categoryId = ca.id
    where m.deletedAt is null 
      and mdi.deletedAt is null 
      and (m.cohortId > 0 or m.cohortId is null)
),

-- break off and rejoin the elation and acpny ids as new fields
base_no_ehr as (
    select * from base 
    where memberIdSource in ('connecticare', 'emblem', 'tufts', 'bcbsNC', 'carefirst', 'cardinal')
),

ehr_ids as (

    select 
        patientId, 
        max(case when mrn.name = 'elation' 
              then mrn.mrn 
              else cast(null as string) 
              end) as elationId,
        max(case when mrn.name = 'acpny' 
              then mrn.mrn 
              else cast(null as string) 
              end) as acpnyId,
        max(case when memberIdSource like 'medicaid%'
              then memberId 
             else cast(null as string) 
             end) as medicaidId,
        max(case when memberIdSource like 'medicare%'
              then memberId 
             else cast(null as string) 
             end) as medicareId
    from base
    LEFT JOIN {{ source('member_index', 'mrn') }} mrn 
      ON mrn.id = base.mrnId
    where mrn.deletedAt is null
    group by patientId
),

result as (
    select distinct
      b.*,
      e.acpnyId,
      e.elationId,
      e.medicaidId,
      e.medicareId,
      pa.firstName,
      pa.lastName, 
      pa.dateOfBirth
    from base_no_ehr b
    left outer join ehr_ids e 
      using(patientId)
    left outer join {{ source('commons', 'patient') }} pa 
      on pa.id = patientId
)

select * from result
order by patientId, memberId
