-- Do not report HTR is the member's state was disenrolled during the time of sla reporting period
with member_historical_state as (
    select distinct patientId,
                    historicalState
      from {{ ref('member_historical_states') }} mh
      inner join {{ ref('thpp_sla_reporting_month') }} on mh.calendarDate between tuftsSlaReportStartDate and tuftsSlaReportEndDate

    where  historicalState = "disenrolled"

),

member_states as (

     select
        m.patientId,
        date(m.attributedAt) as latestAttributedDate,
        date(m.consentedAt) as consentDate,
        m.isReattributed

    from {{ ref('member_states') }} m
    left join member_historical_state mh
        using (patientId)

     where mh.patientId is null

)



select * from member_states
