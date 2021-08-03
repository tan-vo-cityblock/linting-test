with info as (
  select patientId, 
  patientName
  from `cityblock-analytics.mrt_commons.member` ),

claims as (
  select patientId,
  cast(name as string) as medicationName,
  cast(start_date as string) as medicationStartDate,
  cast(end_date as string) as medicationEndDate,
  cast(code as string) as medicationCode,
  cast(quantity_dispensed as string) as medicationQuantity,
  cast(days_supply as string) as dailyDosage,
  cast(provider_npi as string) as providerNpi
  from `cityblock-data.abstractions.medications` ),

ehr as (
  select patient.patientId as patientId,
  cast(medication.Product.Name as string) as medicationName,
  cast(medication.StartDate.date as string) as medicationStartDate,
  cast(medication.EndDate.date as string) as medicationEndDate,
  cast(medication.Product.Code as string) as medicationCode,
  cast(medication.Dose.Quantity as string) as dailyDosage
  from `cityblock-analytics.abs_medical.latest_patient_medications` ),

claims_2 as (
  select i.patientId as memberId,
  i.patientName as memberName,
  --'claims' as dataSource,
  c.medicationName,
  c.dailyDosage,
  c.medicationStartDate, 
  c.medicationEndDate
  --c.medicationQuantity,
 -- c.medicationCode,

 -- c.providerNpi
  from info i
  inner join claims c
  on i.patientId = c.patientId ), 
  
ehr_2 as (
  select i.patientId as memberId,
  i.patientName as memberName,
  --'ehr' as dataSource,
  e.medicationName,
  e.dailyDosage,
  e.medicationStartDate, 
  e.medicationEndDate
 -- '' as medicationQuantity,
 --e.medicationCode,

 -- '' as providerNpi
  from info i
  inner join ehr e
  on i.patientId = e.patientId ),
  
final as (
  select * from claims_2
  union all
  select * from ehr_2 )

select * from final
order by medicationStartDate
