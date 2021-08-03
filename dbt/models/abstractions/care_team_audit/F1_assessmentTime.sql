with consented_members as (

  select 
    patientId, 
    consentedAt
  from {{ ref('member_states') }}
  where consentedAt is not null

),

earliest_answers as (

  select
    patientId,
    minEssentialsStartedAt as firstBaselineQAt
  from {{ ref('essential_tools') }}

),

final as (

  select
    cm.patientId,
    cm.consentedAt,
    ea.firstBaselineQAt,
    date_diff(date(ea.firstBaselineQAt), date(cm.consentedAt), day) as consentedToFirstBaselineQ
  from consented_members cm
  left join earliest_answers ea
  using (patientId)

)

select * from final
