{%- set pulmonary_substrings = ['R05','R06','R09', 'R50'] -%}

with events as (

  select * from {{ ref('mrt_toc_events') }}

),

admits as (

  select * from events where eventType = 'Admit'

),

discharges as (

  select * from events where eventType = 'Discharge'

),

event_base as (
  select patientId, visitId, visitType, max(covidLabResult) as covidLabResult from (

      select patientId, visitId, visitType,covidLabResult from admits
      union distinct
      select patientId, visitId, visitType, covidLabResult from discharges          )

  group by 1,2,3
),

visits as (

  select
    concat(eb.patientId, eb.visitId, eb.visitType) as patientVisitTypeId,
    eb.patientId,
    eb.visitId,
    eb.visitType,
    coalesce(a.locationName, d.locationName) as locationName,
    a.consentedAt,
    a.disenrolledAt,
    coalesce(a.eventReceivedAt, d.eventReceivedAt) as earliestReceivedAt,
    a.eventAt as admitAt,
    a.eventReceivedAt as admitReceivedAt,
    a.eventReceivedWithinThreeDays as admitReceivedWithinThreeDays,
    a.memberStateCategoryAtEventReceipt as memberStateAtAdmitReceipt,
    d.eventAt as dischargeAt,
    d.eventReceivedAt as dischargeReceivedAt,
    coalesce(a.eventAt, d.eventAt) as serviceAt,
    d.eventReceivedWithinThreeDays as dischargeReceivedWithinThreeDays,
    d.memberStateCategoryAtEventReceipt as memberStateAtDischargeReceipt,
    a.dx as admitDiagnoses,
    d.dx as dischargeDiagnoses,

    case 
     when eb.covidLabResult = 2 
      then 'positive'
     when eb.covidLabResult = 1 
      then 'inconclusive'
     when eb.covidLabResult = 0
      then 'negative'
    end as covidLabResult ,

    case 
     when (d.dx is null or not exists(select * from unnest(d.dx) as x where x !='')) and
          ((a.dx is null or not exists(select * from unnest(a.dx) as x where x !=''))) 
      then true 
      else false 
    end as missingDiagnosis


  from event_base eb

  left join admits a
    using (patientId, visitId, visitType)

  left join discharges d
    using (patientId, visitId, visitType)

),

final as (

  select
    *,
    
    case

      {% for diagnosisType in ['admit', 'discharge'] %}

      when 
        exists(
          select *
          from unnest({{ diagnosisType }}Diagnoses) as x
          where
            substr(x,0,3) in {{ list_to_sql(pulmonary_substrings) }} or
            substr(x,0,1)='J'
        )

        then true 

      {% endfor %}

      else false
    end as hasPulmonaryDiagnosis

  from visits

)

select * from final
