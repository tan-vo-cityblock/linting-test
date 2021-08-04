
with events as (

  select *,
    date(eventAt, 'America/New_York') as eventDate
  from {{ ref('abs_medical_events') }}
  where memberStateCategoryAtEventReceipt != 'disenrolled'

),

ranked_events as ( -- rank events by same patient, date, visit, and event to identify duplicates

    select *,
      rank() over(
        partition by patientId, eventDate, visitType, eventType
        order by eventSource, eventReceivedAt desc, visitId
      ) as rnk
    from events

),

toc_events as ( --if duplicate events are received, grab the first one that came in, prioritizing HIE over prior auth

  select * except (rnk)
  from ranked_events
  where rnk = 1

),

apply_initial_flags as (

    select *,
      case
        when eventType = 'Admit'
        and visitType = 'Inpatient'
        then true
        else false
      end as admissionFlag,
      case
        when eventType = 'Admit'
        and visitType = 'Emergency'
        then true
        else false
      end as edFlag,
      case
        when covidLabResult is not null
        or lower(locationName) like '%bio%'
        or lower(locationName) like '%covid%'
        or lower(locationName) like '%lab%'
        then true
        else false
      end as labFlag,
      case
        when eventType = 'Admit'
        and visitType = 'Inpatient'
        and timestamp_diff(current_timestamp, eventAt, day) between 3 and 20 --occurred within 3-20 days
        and lead(eventType) over (partition by patientId order by eventAt) is null --and we don't see a subsequent ping
        then true
        else false
      end as currentlyHospitalized
    from toc_events

),

apply_readmission_flag as (

    select *,
      case
        when admissionFlag = true --inpatient admit
        and timestamp_diff(eventAt, lag(eventAt, 1) over (partition by patientId order by eventAt), day) between 0 and 30 --within 30 days
        and lag(eventType,1) over (partition by patientId order by eventAt) = 'Discharge' --of discharge
        then true
        else false
      end as readmissionFlag
    from apply_initial_flags

),

final as (

  select *
  from apply_readmission_flag
)


select *
from final
