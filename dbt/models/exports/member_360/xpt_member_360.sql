with mrt_claims_self_service as (

  select distinct
    {{ dbt_utils.surrogate_key(['patientId', 'claimId', 'lineNumber', 'claimLineStatus']) }} as id,
    patientId,
    claimId,
    lineNumber,
    claimLineStatus,
    placeOfService,
    dateFrom,
    dateTo,
    admissionId,
    dateAdmit,
    dateDischarge,
    date_diff(dateDischarge, dateAdmit, day) + 1 as lengthOfStay
    
  from {{ ref('mrt_claims_self_service') }}
  where partner = 'emblem'
    
),

member_cohorts as (

  select patientId, cohortName
  from {{ ref('src_member') }}
  where cohortName is not null

),

member_delivery_model as (

  select
    patientId,
    currentState as historicalDeliveryModel,
    date(createdAt) as createdDate,
    date(deletedAt) as deletedDate,
    createdAt
    
  from {{ source('commons', 'patient_delivery_model') }}

),

member_historical_states as (

  select
    patientId,
    calendarDate,
    historicalState in ('consented', 'enrolled') as wasConsented
    
  from {{ ref('member_historical_states') }}

),

ranked_delivery_models as (

  select * except (createdAt),
    rank() over(partition by patientId, createdDate order by createdAt desc) as rnk
  
  from member_delivery_model

),

final as (

  select
    c.id,
    c.patientId,
    m.cohortName,
    coalesce(d.historicalDeliveryModel, 'community') as historicalDeliveryModel,
    coalesce(s.wasConsented, false) as wasConsented,
    c.* except (id, patientId)
  
  from mrt_claims_self_service c
  
  inner join member_cohorts m
  using (patientId)
  
  left join ranked_delivery_models d
  on
    c.patientId = d.patientId and
    c.dateFrom >= d.createdDate and
    (c.dateFrom < d.deletedDate or d.deletedDate is null) and
    d.rnk = 1
    
  left join member_historical_states s
  on
    c.patientId = s.patientId and
    c.dateFrom = s.calendarDate
  
)

select * from final
