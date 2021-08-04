--11/11/2020 new field Lili asked to be in the reports: to get one acuity score, use acuity and if its not there then recommended.
--Using the lastest createdat date

with assigned_main as (
    select
        id,
        patient_acuity.patientid,
        score,
        createdAt,
        case when deletedAt <= reporting_datetime then deletedAt else null end as deletedAt
     from {{ source('commons', 'patient_acuity') }}
--we want to make sure createdAt date(s) is between delegation date AND reporting date.
--Other dates act the same but if they are after the reporting date, they are null
      inner join {{ ref('cm_del_reporting_dates_ytd') }}
        on date(createdAt) <= reporting_date
    INNER join {{ ref('cm_del_delegation_dates_ytd') }} dd
     on date(createdat) >= delegation_at
     and patient_acuity.patientid = dd.patientid
),


assigned_rank as(
  select
    *,
    rank () over (
      partition by patientid, createdAt
      order by deletedAt is not null asc, deletedAt desc
    ) as rank_assigned
  from assigned_main
),

recommend_main as (
    select
        id,
        recommended_patient_acuity.patientid,
        acuity,
        createdAt,
        case when deletedAt <= reporting_datetime then deletedAt else null end as deletedAt
     from {{ source('commons', 'recommended_patient_acuity') }}
      inner join {{ ref('cm_del_reporting_dates_ytd') }}
        on date(createdAt) <= reporting_date
     INNER join {{ ref('cm_del_delegation_dates_ytd') }} dd
     on date(createdat) >= delegation_at
     and recommended_patient_acuity.patientid = dd.patientid
),

recommended_rank as (
  select
    *,
    rank () over (
      partition by patientid, createdAt
      order by deletedAt is not null asc, deletedAt desc
    ) as rank_recommended
  from recommend_main

),

latest_date_assigned as (
select
    patientid,
    score as acuity_score,
    row_number()
        over (partition by patientid
        order by createdat desc) as latest_score_assigned,
from  assigned_rank
where rank_assigned = 1
),

latest_date_recommended as (
select
    patientid,
    acuity as recommened_acuity_score,
    row_number()
        over (partition by patientid
        order by createdat desc) as latest_score_recommended,
  from recommended_rank
  where rank_recommended = 1
),

recommend_assigned_score as (
    select pt.patientid,
        coalesce(la.acuity_score,lr.recommened_acuity_score) as final_acuity
    from {{ ref('cm_del_patients_ytd') }} pt
    left join latest_date_assigned la
        on pt.patientid = la.patientid
        and la.latest_score_assigned = 1
    left join latest_date_recommended lr
        on pt.patientid = lr.patientid
        and lr.latest_score_recommended = 1
),

care_management as (
    select
        patientid,
        final_acuity,
        case
            when final_acuity in (1,2) then 'Regular'
            when final_acuity in (3,4,5) then 'Complex'
        else null end as care_management_model
    from recommend_assigned_score
)

select
*,
current_date as date_run
from care_management
