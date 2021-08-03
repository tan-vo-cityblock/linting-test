
with claim_dates as (

  select distinct 
    patientId, 
    dateAdmit as admitDate

  from {{ ref('mrt_claims_self_service') }}

  where
    acuteInpatientCategoryFlag is true and
    date_diff(maxDateFrom, dateAdmit, day) >= 90
    
),

member_start_dates as (

  select
    patientId,
    coalesce(cohortGoLiveDate, date(createdAt, 'America/New_York')) as startDate
    
  from {{ ref('src_member') }}

),

claim_w_state_dates as (

  select cd.*

  from claim_dates cd

  inner join member_start_dates msd
  using (patientId)

  where cd.admitDate >= msd.startDate

),

abs_medical_events as (

  select distinct
    patientId,
    eventSource,
    date(eventAt, "America/New_York") as admitDate
    
  from {{ ref('abs_medical_events') }}
  
  where
    visitType = 'Inpatient' and
    eventType = 'Admit'

),

hie_alerts as (

  select patientId, admitDate
  from abs_medical_events
  where eventSource = 'hie'

),

emblem_cci_prior_authorizations as (

  select patientId, admitDate
  from abs_medical_events
  where eventSource = 'prior auth'

),

tufts_prior_authorizations as (

  select distinct
    patientId,
    admissionDate as admitDate

  from {{ ref('abs_prior_auth_tufts_latest') }}

  where admissionDate is not null

),

prior_authorizations as (

  select * from emblem_cci_prior_authorizations
  union all
  select * from tufts_prior_authorizations

),

final as (

  {% set source_list = ['hie_alerts', 'prior_authorizations'] %}

  {% for source_name in source_list %}

    select
      claims.patientId,
      claims.admitDate,
      '{{ (source_name | replace("_", " "))[:-1] }}' as notificationSource,
      notifications.admitDate is not null as hasNotification

    from claim_w_state_dates claims

    left join {{ source_name }} notifications
    using (patientId, admitDate)

    {% if not loop.last %} union all {% endif %}

  {% endfor %}

)

select * from final
