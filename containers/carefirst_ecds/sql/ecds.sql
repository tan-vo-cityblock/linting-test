with encounters as (
select
src_patient_encounters_latest.messageid as ccd_encounter_id,
src_patient_encounters_latest.patient.patientid as patientid,
provider.firstName||" "||provider.lastname as provider_name,
case when provider_id.idtype = 'npi' then provider_id.id
else null end as npi,
provider.address.street1||" "||provider.address.street2||", "||provider.address.city||", "||provider.address.state||" "||provider.address.zip as provider_address,
locations.type.code,
src_patient_encounters_latest.encounter.type.name as encounter_type,
reasons.name as reason_for_visit,
diagnosis.name as diagnosis_name,
reasons.notes,
date(src_patient_encounters_latest.encounter.dateTime.date) as encounterDatetime,
date(src_patient_encounters_latest.insertedat) as insertedat,
from `cityblock-analytics.src_medical.src_patient_encounters_latest` src_patient_encounters_latest
    left join unnest(encounter.providers) as provider
    left join unnest(provider.identifiers) as provider_id
    left join unnest(encounter.locations) as locations
    left join unnest(encounter.reasons) as reasons
    left join unnest (encounter.diagnoses) as diagnosis
where date(src_patient_encounters_latest.encounter.dateTime.date) between date_trunc(date_sub(current_date, interval 1 month), month) and last_day(date_sub(current_date, interval 1 month), month)
),


final as (select
   member.patientId,
   member.memberId as Medicaid_ID,
   member.partnerName,
   clinic.id as clinic_id,
   clinic.name as clinic_name,
   clinic.departmentId as clinic_department_id,
   date(patient_clinic_history.createdAt) as patient_clinic_history_createdAt,
   isWhite,
   isBlack,
   isAmericanIndianAlaskan,
   isAsian,
   isHawaiianPacific,
   isOtherRace,
   isHispanic,
   encounters.* except (patientid)
from `cbh-db-mirror-prod.commons_mirror.patient_info` patient_info
inner join `cityblock-analytics.mrt_commons.member` member using (patientid)
left join encounters using (patientid)
left join `cbh-db-mirror-prod.commons_mirror.patient_clinic_history` patient_clinic_history  using (patientid)
left join `cbh-db-mirror-prod.commons_mirror.clinic` clinic
   on clinic.id = patient_clinic_history.clinicId
where  member.patienthomemarketname = 'CareFirst DC'
order by patientid
)

select * from final