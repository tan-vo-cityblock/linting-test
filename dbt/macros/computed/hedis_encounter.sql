

{% macro hedis_encounter(slug, period="1 YEAR", table=ref('abs_encounters')) %}

with agg as (

    select
        patientId,
        array_agg(
          struct(
            date,
            ed,
            obs,
            inpatient,
            resultsInInpatient
          )
        ) as enc

    from {{ table }}

    where date >= date_sub(current_date('America/New_York'), interval {{ period }} )

    group by patientId

),

counts as (

    select
        patientId,

        (
          select count(distinct date)
          from unnest(enc)
          where (inpatient or obs)
        ) as inp_count,

        (
          select count(distinct date)
          from unnest(enc)
          where ed and not resultsInInpatient
        ) as ed_count

    from agg

),

cf_status as (

    select
        patientId,

        case
          when inp_count >= 2  or ed_count >= 3 then "acute utilization severe"
          when inp_count  = 1  or ed_count  = 2 then "acute utilization moderate"
          when inp_count  = 0 and ed_count  = 1 then "acute utilization mild"
          when inp_count  = 0 and ed_count  = 0 then "acute utilization stable"
        end
        as value

    from counts

),

final as (

    {{ computed_field(slug=slug, type='hedis_encounter', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
