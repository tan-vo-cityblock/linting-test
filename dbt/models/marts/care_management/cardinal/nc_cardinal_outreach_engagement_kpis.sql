--Outreach/Engagement KPIs - NC CARDINAL
with member_state_count as (
    select
        ms.patientId,
        ms.currentState,
        ms.contactAttemptedAt,
        ms.attributedAt,
        ms.consentedAt,
        ms.reachedAt,
        ms.disenrolledAt
    from {{ ref('member_states') }} ms
    left join {{ ref('member') }} m
    using (patientId)
    where m.partnerName = "Cardinal" and m.patientHomeMarketName = "North Carolina"


),

--KPI1: 85% attempt to contact all assigned Cohort Members within 30 days of assignment to Cityblock, as defined by at least one phone call

-- Numerator: # of members with a contact attempt
-- Denominator : All non disenrolled members

earliest_phone_outreach as (

  select
    patientId,
    min(interactionAt) as earliestOutreachPhoneCallAt
  from {{ ref('stg_commons_outreach_notes') }}
  where
    direction = 'outbound' and
    groupedModality = 'phoneCall'
  group by 1

),

kpi1_deno as (

   select
    patientId,
    attributedAt
    from member_state_count msc
    where disenrolledAt is null

),
kpi1_num as (

  select patientId
  from kpi1_deno d
  inner join earliest_phone_outreach o
  using (patientId)
  where timestamp_diff(o.earliestOutreachPhoneCallAt, d.attributedAt, day) <= 30

),

--KPI2: 40% of Cohort Members successfully contacted are engaged and/or consented for Care
--Numerator: # of members consented

--Denom: # of members reached
 kpi2_deno as (

 select
    patientId,
    consentedAt
    from member_state_count msc
    where reachedAt is not null

 ),

 kpi2_num as (

    select
        patientId
    from kpi2_deno
    where consentedAt is not null

 ),



 {% for kpi_Id in [ '1', '2' ] %}

kpi{{ kpi_Id }}RateCalc as

(select
           round((count(kpi{{ kpi_Id }}_num.patientId)/nullif(count(kpi{{ kpi_Id }}_deno.patientId),0))*100,2) as kpi{{ kpi_Id }}Rate,
    from kpi{{ kpi_Id }}_deno
    left join kpi{{ kpi_Id }}_num using (patientId)

) ,


{% endfor %}

final as (

  select *
  from kpi1RateCalc
  cross join kpi2RateCalc

)

select * from final

