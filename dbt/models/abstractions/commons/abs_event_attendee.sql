
{{ config(materialized = 'ephemeral') }}

with attendees as (

  select
    eventGroupId,
    case
      when idSource = 'patient'
        then 'Member'
      else attendee
    end as attendee
    
  from {{ source('commons', 'event_attendee') }}
  
  where deletedAt is null

),

aggregated_attendees as (

  select
    eventGroupId,
    string_agg(distinct attendee, ", " order by attendee) as attendees

  from attendees

  group by eventGroupId

)

select * from aggregated_attendees
