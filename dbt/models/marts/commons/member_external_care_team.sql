
with cleaned_external_providers as ( --to do: include other degree types
  select
  patientId,
  id as patientExternalProviderId,
  case
  when lower(trim(firstName)) like 'dr.%' then right(trim(firstName), length(trim(firstName)) - 3) --addresses cases for 'Dr.' or 'dr.' or 'DR.'
  when lower(trim(firstName)) like 'dr' then right(trim(firstName), length(trim(firstName)) - 2) --addresses cases for when firstName = 'Dr' or 'dr' or 'DR'
  when regexp_contains(trim(firstName),'Dr[[:blank:]]') then right(trim(firstName), length(trim(firstName)) - 2) --addresses cases when firstName = 'Dr <Name>'
  else trim(firstName)
  end as firstName,
  case
  when lastName like '%MD%' then replace(trim(lastName),'MD','')
  when lastName like '%M.D.%' then replace(trim(lastName),'M.D.','')
  when regexp_contains(trim(lastName),'[[:blank:]]NP-C') then replace(trim(lastName),'NP-C','') --addresses cases for ' NP-C'
  when regexp_contains(trim(lastName),'[[:blank:]]NP') then replace(trim(lastName),'NP','') --addresses cases for ' NP'
  when regexp_contains(trim(lastName),',NP') then replace(trim(lastName),',NP','') --addresses cases for ',NP'
  when lastName like '%,%' then replace(trim(lastName),',','')
  else trim(lastName)
  end as lastName,
  coalesce(roleFreeText, role) as providerRole,
  patientExternalOrganizationId
from {{ source('commons', 'patient_external_provider') }}
where deletedAt is null

),

external_providers as (

  select
    patientId,
    patientExternalProviderId,
    case
    when lastName like '%,%' then replace (trim(trim(coalesce(trim(firstName), 'Unknown')) || ' ' || trim(coalesce(trim(lastName),'Unknown')) ),',','')
    else trim(trim(coalesce(trim(firstName), 'Unknown')) || ' ' || trim(coalesce(trim(lastName),'Unknown')) )
    end as providerName,
    providerRole,
    patientExternalOrganizationId
    from cleaned_external_providers

),

organizations as (

  select
    id as patientExternalOrganizationId,
    name as organizationName
  from {{ source('commons', 'patient_external_organization') }}
  where deletedAt is null

),

final as (

  select
    ep.patientId,
    ep.patientExternalProviderId,
    ep.providerName,
    ep.providerRole,
    o.organizationName
  from external_providers ep
  left join organizations o
  using (patientExternalOrganizationId)

)

select * from final
