
{{
  config(
    materialized='ephemeral'
  )
}}

with consent_dates as (

  select 
    ms.patientId,
    ms.consentedAt
  from {{ ref('member_states') }} ms
  inner join {{ ref('member') }} m
  using (patientId)
  where ms.consentedAt is not null

),

answer_dates as (

  select 
    pa.patientId, 
    min(pa.createdAt) as minAnswerAt
  from {{ source('commons', 'patient_answer') }} pa
  where pa.questionSlug = 'list-your-current-medications-including-dose-and-frequency'
  group by pa.patientId

),

med_rec_codes as (

  select code as procedureCode
  from {{ source('hedis_codesets', 'vsd_value_set_to_code') }}
  where value_set_name = 'Medication Reconciliation'

),

cbh_npis as (

  select npi
  from {{ ref('src_cityblock_providers') }}
  where overallStatus = 'Active'
  
),

proc_dates as (

  select
    d.memberIdentifier as patientId,
    timestamp(min(d.serviceDateFrom)) as minProcAt
  from {{ ref('abs_procedures') }} d
  inner join med_rec_codes
  using (procedureCode)
  inner join cbh_npis cbh
  on d.providerNpi = cbh.npi
  inner join consent_dates cd
  on
    d.memberIdentifier = cd.patientId and
    d.serviceDateFrom > date(cd.consentedAt)
  where d.memberIdentifierField = 'patientId'
  group by d.memberIdentifier
  
),

final as (

  select 
    cd.patientId,
    greatest(ad.minAnswerAt, pd.minProcAt) as verifiedMedListAt
  from consent_dates cd
  inner join answer_dates ad
  using (patientId)
  inner join proc_dates pd
  using (patientId)

)

select * from final
