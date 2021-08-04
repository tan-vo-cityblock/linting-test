
with next_admits as (

  select distinct
    patientId,
    date(admitAt) as admitDate,
    date(DischargeAt) as dischargeDate,
    visitType,
    patientVisitTypeId,

    lead(visitType) over(

      partition by patientId
      order by admitAt, visitType in ('Emergency', 'Observation') desc

    )

    as nextVisitType,

    date(lead(admitAt) over(

      partition by patientId
      order by admitAt, visitType in ('Emergency', 'Observation') desc)

    )

    as nextVisitAdmitDate

  from {{ ref('abs_admit_discharge_events_combined') }}

),

encounters as (

  select distinct
    patientId,
    admitDate as date,
    dischargeDate as dischargeDate,
    visitType = 'Emergency' as ed,
    cast(null as boolean) as acsEd,
    visitType = 'Observation' as obs,
    visitType = 'Inpatient' as inpatient,
    cast(null as boolean) as acsInpatient,
    cast(null as boolean) as bhInpatient,
    patientVisitTypeId,

    coalesce(

      visitType in ('Emergency', 'Observation') and
      nextVisitType = 'Inpatient' and
      date_diff(nextVisitAdmitDate, admitDate, day) <= 2, FALSE

    ) as resultsInInpatient

  from next_admits

)

select * from encounters
