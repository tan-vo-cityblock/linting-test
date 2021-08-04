
with latest_authorizations as (

  select * from {{ ref('abs_prior_auth_tufts_latest') }}

),

diagnosis_code_descriptions as (

  select
    code as diagnosisCode,
    name as diagnosisCodeDescription
  
  from {{ source('codesets', 'icd10cm') }}

),

procedure_code_descriptions as (

  select
    cpt_hcpcs_code as procedureCode,
    cpt_hcpcs_desc as procedureCodeDescription
  
  from {{ source('claims_descriptions', 'cpt_hcpcs_lo_cu_v1') }}

),

ranked_addresses as (

  select distinct
    authorizationNumber,
    memberAddress,
    memberCity,
    memberState,
    memberZip,
    rank() over(
      partition by authorizationNumber 
      order by length(memberAddress) desc, memberAddress, memberCity
    ) as rnk
  
  from latest_authorizations

),

selected_addresses as (

  select * except (rnk)
  
  from ranked_addresses 
  
  where rnk = 1

),

split_diagnosis_codes as (

  select
    authorizationNumber,
    split(diagnosisCodes, ":") as diagnosisCodes
  
  from latest_authorizations 

),

flattened_diagnosis_codes as (

  select distinct
    authorizationNumber,
    diagnosisCode
  
  from split_diagnosis_codes 
  
  left join unnest(diagnosisCodes) as diagnosisCode

),

total_approved_procedure_counts as (

  select
    authorizationNumber,
    procedureCode,
    modifierCode,
    sum(approvedProcedureCount) as totalApprovedProcedureCount
    
  from latest_authorizations
  
  where procedureCode is not null
  
  group by authorizationNumber, procedureCode, modifierCode

),

authorized_bed_type_date_ranges as (

  select
    authorizationNumber,
    authorizedBedType,
    authorizedBedTypeDescription,
    authorizedBedTypeStatus,
    min(authorizedBedTypeStartDate) as minAuthorizedBedTypeStartDate,
    max(authorizedBedTypeEndDate) as maxAuthorizedBedTypeEndDate
  
  from latest_authorizations 
  
  {{ dbt_utils.group_by(n = 4) }}

),

final as (

  select distinct
    a.fileName, 
    a.receivedDate,
    a.authorizationNumber,
    a.authorizationStatus,
    a.authorizationCertificationDate,
    a.authorizationStartDate,
    a.authorizationEndDate,
    a.patientId,
    a.externalId,
    a.memberName,
    sa.memberAddress,
    sa.memberCity,
    sa.memberState,
    sa.memberZip,
    a.requestingProviderNpi,
    a.requestingProviderName,
    a.requestingProviderAddress1,
    a.requestingProviderAddress2,
    a.requestingProviderCity,
    a.requestingProviderState,
    a.requestingProviderZip,
    a.requestingProviderPhone,
    a.servicingProviderNpi,
    a.servicingProviderName,
    a.servicingProviderAddress1,
    a.servicingProviderAddress2,
    a.servicingProviderCity,
    a.servicingProviderState,
    a.servicingProviderZip,
    a.servicingProviderPhone,
    d.diagnosisCode,
    dd.diagnosisCodeDescription,
    a.procedureCode,
    pd.procedureCodeDescription,
    nullif(a.modifierCode, '') as modifierCode,
    p.totalApprovedProcedureCount,
    a.admissionDate,
    a.facilityNpiTin,
    a.approvedLengthOfStay,
    bt.authorizedBedType,
    bt.authorizedBedTypeDescription,
    bt.authorizedBedTypeStatus,
    bt.minAuthorizedBedTypeStartDate,
    bt.maxAuthorizedBedTypeEndDate
  
  from latest_authorizations a
  
  left join selected_addresses sa
  using (authorizationNumber)
  
  left join flattened_diagnosis_codes d
  using (authorizationNumber)

  left join diagnosis_code_descriptions dd
  using (diagnosisCode)

  left join procedure_code_descriptions pd
  using (procedureCode)
  
  left join total_approved_procedure_counts p
  using (authorizationNumber, procedureCode, modifierCode)
  
  left join authorized_bed_type_date_ranges bt
  using (authorizationNumber)

)

select * from final
