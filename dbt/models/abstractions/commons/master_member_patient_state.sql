{{
 config(
   materialized='table'
 )
}}

-- creating a patient state/disenrolled table for member management
-- aseiden 7/31/19

with states as (
   select distinct
       mhs.patientId,
       mhs.historicalState as commonsState,
       d.reason as disenrollmentReason,
       d.note as disenrollmentNote,
       extract(date from mhs.patientStateCreatedAt at time zone 'EST') as spanFromDate,
       extract(date from IFNULL(mhs.patientStateDeletedAt, CURRENT_TIMESTAMP()) at time zone 'EST') as spanToDate,
       mhs.patientStateCreatedAt as spanFromTimestamp,
       IFNULL(mhs.patientStateDeletedAt, CURRENT_TIMESTAMP()) as spanToTimestamp
   from {{ ref('member_historical_states') }} as mhs
   left outer join {{ source('commons', 'patient_disenrollment') }} as d
     on mhs.patientId = d.patientId
     and extract(date from mhs.patientStateCreatedAt at time zone 'EST') = extract(date from d.createdAt at time zone 'EST')
     and mhs.historicalState like 'disenrolled%'
   order by patientId, spanFromDate, SpanToDate 
),

-- joining back onto the original table but only keeping the rows that match the max time per member per day
result as (
 select
    s.patientId,
    s.commonsState,
    if( s.commonsState like 'disenrolled%', (lag(s.commonsState) over (PARTITION BY s.patientId ORDER BY s.spanFromDate ASC)), null) as commonsStatePriorToDisenrollment,
    s.disenrollmentReason,  
    s.disenrollmentNote,  
    s.spanFromDate,
   --  subtracting 1 unless it's today, so that there are no overlapping dates, and spanto of the prior line is always -1 day before
    if(s.spanToDate = current_date(), spanToDate, date_add(spanToDate, interval - 1 day)) as spanToDate
 from states as s
 order by patientId, spanFromDate, SpanToDate 
)

select * from result
order by patientId, spanFromDate, SpanToDate 



