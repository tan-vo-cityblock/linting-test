
with claims_encounters as (

  select distinct
    patientId,
    admissionId,
    dateFrom as date,
    dateTo,
    edServiceFlag as ed,
    acsEdFlag as acsEd,
    obsCategoryFlag as obs,
    acuteInpatientCategoryFlag as inpatient,
    acsInpatientFlag as acsInpatient,
    costCategory = 'inpatient' and costSubCategory = 'psych' as bhInpatient,
    resultsInInpatientFlag as resultsInInpatient

  from {{ ref('mrt_claims_self_service') }} 

)

select * from claims_encounters
