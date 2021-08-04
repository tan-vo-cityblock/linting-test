with diagnoses as (

  select distinct
    memberIdentifier as patientId,
    diagnosisCode

  from {{ ref('abs_diagnoses') }}

  where memberIdentifierField = 'patientId'

),

diagnosis_groups as (

  select
    icd_10_cm_code as diagnosisCode,
    multi_ccs_lvl_2 as diagnosisGroup
  
  from {{ ref('src_reference_ccs_dx_icd10cm') }}

  where multi_ccs_lvl_2 is not null

),

chronic_codes as (

  select icd_10_cm_code as diagnosisCode
  from {{ source('codesets', 'cci_icd10cm') }}
  where chronic_indicator = '1'

),

exclusions as (

  select code, column
  from {{ ref('dta_medical_chronic_condition_exclusions') }}

),

diagnosis_code_exclusions as (

  select code as diagnosisCode
  from exclusions
  where column = 'diagnosisCode'

),

diagnosis_group_exclusions as (

  select code as diagnosisGroup
  from exclusions
  where column = 'diagnosisGroup'

),

final as (

  select
    d.patientId,
    count(distinct dg.diagnosisGroup) as chronicConditionCount
  
  from diagnoses d
  
  inner join diagnosis_groups dg
  using (diagnosisCode)
  
  inner join chronic_codes
  using (diagnosisCode)
  
  left join diagnosis_code_exclusions dce
  using (diagnosisCode)
  
  left join diagnosis_group_exclusions dge
  using (diagnosisGroup)
  
  where
    dce.diagnosisCode is null and
    dge.diagnosisGroup is null
  
  group by 1

)

select * from final
