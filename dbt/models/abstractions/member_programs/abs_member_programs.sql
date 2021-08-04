

with member as (

    select patientId 
    from {{ ref('src_member') }}

),

outreach_prio as (

    select
        patientId,
        true as outreachPriority

    from {{ ref('abs_outreach_prioritization') }}

    where 
        isPriority = true and
        deletedDate is null

    group by patientId

),

merged as (

    select
        member.patientId,
        ifnull(outreach_prio.outreachPriority, false) as outreachPriority

    from member

    left join outreach_prio
      on member.patientId = outreach_prio.patientId

)

select * from merged
