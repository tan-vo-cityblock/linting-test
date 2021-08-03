select * from {{ ref('mrt_claims_self_service_all') }}
where patientId != 'deidentified'
