with elation_vax as (
select distinct
    cast(patient_id as string) patient_id,
    name as vaccineName,
    administered_date as dateAdministered,
    manufacturer_name as manufacturer,
    expiration_date,
    method,
    dense_rank() over (
                        partition by pi.patient_id
                        order by pi.administered_date ASC
                        ) as dose_number
from {{ source('elation_mirror','patient_immunization') }} as pi
where 
    pi.cvx = 212 or pi.cvx = 213
    and deletion_time is null
),

epic_vax as(
select distinct
    patientIdentifier,
    patientFirstName,
    patientLastName,
    patientDOB,
    dateTime as dateAdministered,
    manufacturer,
    name as vaccineName,
    dense_rank() over (
                        partition by patientIdentifier
                        order by dateTime ASC 
                        ) as dose_number
from {{ ref('src_acpny_immunizations') }} 

where name in ('Moderna Covid-19', 'Pfizer Covid 19', 'Janssen COVID-19 Vaccine')
)

select distinct 
    p.Id as patientId,
    p.memberID,
    p.lastName,
    p.firstName,
    p.dateOfBirth as DOB,
    mkt.name as marketName,
    c.name as clinicName,
    cast(ev1.dateAdministered as STRING) as dose_1,
    ev1.manufacturer as dose_1_manufacturer,
    ev1.vaccineName as dose_1_name,
    cast(ev2.dateAdministered as STRING) as dose_2,
    ev2.manufacturer as dose_2_manufacturer,
    ev2.vaccineName as dose_2_name,
    'Elation' as source
from {{ source('commons','patient') }} as p
join (select patient_id,dateAdministered,manufacturer,vaccineName
       from elation_vax
       where dose_number = 1) ev1
    on ev1.patient_id = p.mrn
left join (select patient_id,dateAdministered,manufacturer,vaccineName
       from elation_vax
       where dose_number = 2) ev2
    on ev2.patient_id = p.mrn
left join {{ source('commons','market') }} as mkt
    on mkt.id = p.homeMarketId
left join {{ source('commons','clinic') }} as c
    on c.id = p.homeClinicId

union all

select distinct 
    mmt.patientId,
    mmt.latestMemberId as memberId,
    ifnull(mmt.lastName,epv1.patientLastName) as lastName,
    ifnull(mmt.firstName,epv1.patientFirstName) as firstName,
    ifnull(mmt.dateOfBirth,cast(epv1.patientDOB as date)) as DOB,
    mmt.marketName,
    mmt.clinicName,
    epv1.dateAdministered as dose_1,
    epv1.manufacturer as dose_1_manufacturer,
    epv1.vaccineName as dose_1_name,
    epv2.dateAdministered  as dose_2,
    epv2.manufacturer as dose_2_manufacturer,
    epv2.vaccineName as dose_2_name,
    'Epic' as Source
from {{ ref('master_member_v1') }} as mmt
join (select patientFirstName,patientLastName,patientDOB,dateAdministered,manufacturer,vaccineName
      from epic_vax
      where dose_number = 1) epv1
    on UPPER(epv1.patientFirstName) = mmt.firstName 
        and UPPER(epv1.patientLastName) = mmt.lastName
        and cast(epv1.patientDOB as date) = mmt.dateOfBirth
left join (select patientFirstName,patientLastName,patientDOB,dateAdministered,manufacturer,vaccineName
      from epic_vax
      where dose_number = 2) epv2
    on UPPER(epv2.patientFirstName) = mmt.firstName 
        and UPPER(epv2.patientLastName) = mmt.lastName
        and cast(epv2.patientDOB as date) = mmt.dateOfBirth

UNION ALL 

select distinct
    mmt.patientId,
    mmt.latestMemberId as memberId,
    ma.LAST_NAME as lastName,
    ma.FIRST_NAME as firstName,
    mmt.dateOfBirth as DOB,
    mmt.marketName,
    mmt.clinicName,
    ma.Covid_vaccine_dose1_date as dose_1,
    '' as dose_1_manufacturer,
    ma.covid_vaccine_drug as dose_1_name,
    ma.Covid_vaccine_dose2_date as dose_2,
    '' as dose_2_manufacturer,
    case when ma.covid_vaccine_dose2_date is not null 
        then ma.covid_vaccine_drug
        else null
        end as dose_2_name,
    'Tufts' as Source
from {{ source('covid_immunization_extracts','covid_vaccine_status_ma') }} as ma
join {{ ref('master_member_v1') }} as mmt 
    ON mmt.memberId = ma.MEM_ID

union all 

select distinct
    mmt.patientId,
    mmt.latestMemberId as memberID,
    dc.MEM_LAST_NAME as lastName,
    dc.MEM_FIRST_NAME as firstName,
    cast(dc.DOB as DATE ) as DOB,
    mmt.marketName,
    mmt.clinicName,
    dc.First_Dose_Vaccine_Date as dose_1,
    '' as dose_1_manufacturer,
    dc.First_Dose_Vaccine as dose_1_name,
    dc.Final_Dose_Vaccine_Date as dose_2,
    '' as dose_2_manufacturer,
    dc.Final_Dose_Vaccine as dose_2_name,
    'CareFirst/CRISP' as Source
from {{ source('covid_immunization_extracts','covid_vaccine_status_dc') }} as dc
join {{ ref('master_member_v1') }} as mmt 
    on mmt.memberId = dc.Member_ID

union all

select
  distinct mmt.Id as patientId,
  mmt.memberId as memberID,
  mmt.lastName,
  mmt.firstName,
  mmt.dateOfBirth as DOB,
  mkt.name as Market,
  c.name as Clinic,
  st1.answerText as dose_1,
  '' as dose_1_manufacturer,
  st3.answerText as dose_1_name,
  st2.answerText as dose_2,
  '' as dose_2_manufacturer,
  case when st2.patientId is not null then st3.answerText
    else null end as dose_2_name,
  'Covid Screener' as Source
from {{ source('commons','patient') }} as mmt
join (
  select
    patientId,
    answerText,
    rank() over (partition by patientId order by updatedAt desc) as most_recent
  from {{ source('commons','patient_answer') }}
  where
    questionSlug = 'date-of-first-covid-vaccine'
    and deletedAt is null
    ) as st1
on mmt.Id = st1.patientId
    and st1.most_recent = 1
left join (
  select
    patientId,
    answerText,
    rank() over (partition by patientId order by updatedAt desc) as most_recent
  from {{ source('commons','patient_answer') }}
  where
    questionSlug = 'date-of-second-covid-vaccine'
    and deletedAt is null
    ) as st2
on mmt.Id = st2.patientId
    and st2.most_recent = 1
left join (
  select
    patientId,
    answerText,
    rank() over (partition by patientId order by updatedAt desc) as most_recent
  from {{ source('commons','patient_answer') }}
  where
    questionSlug = 'what-brand-of-vaccine-did-you-receive'
    and deletedAt is null
    ) as st3
on mmt.Id = st3.patientId
    and st3.most_recent = 1
left join {{ source('commons','market') }} as mkt
    on mkt.id = mmt.homeMarketId
left join {{ source('commons','clinic') }} as c
    on c.id = mmt.homeClinicId
