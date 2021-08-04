select * replace (cast(id as string) as id),
  cast(patient_id as string) as mrn

from {{ source('elation', 'appointment_latest') }}
where
  patient_id is not null and
  deletion_time is null

