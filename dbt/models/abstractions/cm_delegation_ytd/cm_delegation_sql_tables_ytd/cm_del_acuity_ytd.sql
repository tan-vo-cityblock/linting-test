--Aggregate acuity by patient: for observations pre-delegation, take the latest score (score at time of delegation)
-- this is to give one line of data per member (latest and initial acuity)

with acuity_limited as (
    select
        patient_acuity.patientid,
        createdAt,
        score,
        case when deletedat <= rd.reporting_datetime then deletedat
        else null end as deletedat,
    from {{ source('commons', 'patient_acuity') }}
--we want to make sure createdAt date(s) is between delegation date AND reporting date.
--Other dates act the same but if they are after the reporting date, they are null
    inner join  {{ ref('cm_del_reporting_dates_ytd') }}  rd
    on createdAt <= rd.reporting_datetime
    INNER join {{ ref('cm_del_delegation_dates_ytd') }} dd
     on date(createdat) >= delegation_at
     and patient_acuity.patientid = dd.patientid
),

acuitymain as (
  select
    patientid,
    createdAt,
    score,
    deletedat,
    --this is incase there are multiple max and min createdat for one patient
    rank () over (
      partition by patientid, createdAt
      order by deletedAt is not null asc, deletedAt desc
    ) as rank,
     case when score = 0 then 'No Acuity'
          when score = 1 then 'Stable'
          when score  = 2 then 'Mild'
          when score  = 3 then 'Moderate'
          when score  = 4 then 'Severe'
          when score  = 5 then 'Critical'
     else null end as score_definition
  from acuity_limited
),


first_last as(
select
    pa.patientid,
    createdAt,
    score,
    score_definition,
    row_number()
        over (partition by patientid
        order by createdat desc) as latest,
    row_number()
        over (partition by patientid
        order by createdat asc) as earliest
    from acuitymain pa
    where rank = 1
),

count_a as (
    select
        patientid,
        count (distinct (createdAt)) as count_acuity
    from acuitymain
    where rank = 1
    group by 1
),

score as (
    select
        a.patientid,
        b.score as score_final,
        b.score_definition as score_definition_final,
        count_acuity,
        a.createdat as initial_date,
        a.score as initial_score,
        a.score_definition as initial_acuity,
        b.createdat as latest_date,
    from first_last a
    left join first_last b
    on a.patientid = b.patientid and b.latest = 1
    left join count_a
    on a.patientid = count_a.patientid
    where a.earliest = 1
),


final as (select
    *,
    case
      when count_acuity > 1 then latest_date
      else null end as latest_acuity_date,
    case
      when count_acuity > 1 then score_final
      else null end as latest_acuity_score
from score
)


select
    *,
    case
      when latest_acuity_score is not null then score_definition_final
      else null end as latest_acuity_description,
    current_date as date_run
from final
