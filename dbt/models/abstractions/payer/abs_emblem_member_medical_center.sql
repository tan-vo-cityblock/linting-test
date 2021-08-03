
with ordered_medical_centers as (

  select distinct
    patient.patientId,
    month.MEDICALCENTERNAME as emblemMedicalCenterName,
    month.SPANFROMDATE,
    row_number() over(partition by patient.patientId order by month.SPANFROMDATE desc) as rowNum

  from {{ source('emblem_silver', 'member_month') }}

  where 
    patient.patientId is not null and 
    month.medicalCenterName is not null

),

most_recent as (

  select patientId, emblemMedicalCenterName

  from ordered_medical_centers

  where rowNum = 1

)

select * from most_recent
