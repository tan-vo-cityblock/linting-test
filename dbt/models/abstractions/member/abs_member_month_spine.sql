
with base as (

    select distinct
        patientId,
        dd.yearMonth,
        dd.year,
        dd.month,
        dd.quarter,
        dd.firstDayOfMonth,
        dd.lastDayOfMonth

    from {{ ref('member') }}

    cross join {{ ref('date_details') }} as dd

)

select * from base
