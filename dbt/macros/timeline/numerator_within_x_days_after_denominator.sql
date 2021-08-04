

{#



 #}


{% macro numerator_within_x_days_after_denominator(numerator,
                                                   denominator,
                                                   event_relationship='one_to_one',
                                                   chop_numer_outside_time_min=0,
                                                   chop_numer_outside_time_max=30,
                                                   tdiff_truncated=0,
                                                   daydelta_list=[0,15,30,45,60]) %}

with denominator as (

    select
        struct(
          eventId,
          patientId,
          eventCreatedAt
        ) as denom

    from {{ref('all_fields')}}

    where eventId is not null
      and eventName = '{{ denominator }}'

),

numerator as (

    select
        struct(
          eventId,
          patientId,
          eventCreatedAt
        ) as numer

    from {{ref('all_fields')}}

    where eventId is not null
      and eventName = '{{ numerator }}'

),

-- How do we handle simultaneous numerator events? Do we need to?
many_to_many as (

    select
        d.denom,
        n.numer,

    {% if tdiff_truncated == 0 %}
        timestamp_diff(numer.eventCreatedAt, denom.eventCreatedAt, MINUTE) / 60
        as tdiffHours,

    {% elif tdiff_truncated == 1 %}
        timestamp_diff(
          TIMESTAMP_TRUNC(numer.eventCreatedAt, DAY),
          TIMESTAMP_TRUNC(denom.eventCreatedAt, DAY), HOUR)
        as tdiffHours,

    {% endif %}

        row_number() over (partition by denom.eventId order by numer.eventCreatedAt) as row_num_many_to_one,
        row_number() over (partition by numer.eventId order by denom.eventCreatedAt desc) as row_num_one_to_many -- Ties broken at random

    from denominator d

    left join numerator n
      on d.denom.patientId = n.numer.patientId
        and timestamp_diff(numer.eventCreatedAt, denom.eventCreatedAt, SECOND)
          between {{ chop_numer_outside_time_min }} and {{ chop_numer_outside_time_max }} * 24 * 60 * 60
        and denom.eventId <> numer.eventId

    order by denom.eventId, denom.eventCreatedAt
),

chosen_mapping as (

    select
        denom,
        numer,
        tdiffHours

  from many_to_many

  {% if event_relationship == 'many_to_many' %} -- do not filter with many to many
    where 1=1
  {% elif event_relationship == 'one_to_one' %}
    where (row_num_one_to_many = 1 and row_num_many_to_one = 1 and numer.eventId is not null) or (numer.eventId is null)
  {% elif event_relationship == 'one_to_many' %}
    where (row_num_one_to_many = 1 and numer.eventId is not null) or (numer.eventId is null)
  {% elif event_relationship == 'many_to_one' %}
    where row_num_many_to_one = 1
  {% endif %}

),

dangling_denom as (

    select
        denom.eventId,
        denom.patientId,
        denom.eventCreatedAt

    from many_to_many except distinct

    select denom.eventId, denom.patientId, denom.eventCreatedAt

    from chosen_mapping

),

dangling_denom_transformed as (

    select
        struct(
          eventId,
          patientId,
          eventCreatedAt
        ) as denom,

        struct(
          STRING(NULL) as eventId,
          STRING(NULL) as patientId,
          timestamp(NULL) as eventCreatedAt
        ) as numer,

        NULL as tdiffHours

    from dangling_denom

    union all

    select * from chosen_mapping
),

full_output as (

    select
        denom,
        numer,
        tdiffHours,

        TIMESTAMP_DIFF(
          TIMESTAMP_TRUNC(numer.eventCreatedAt, DAY),
          TIMESTAMP_TRUNC(denom.eventCreatedAt, DAY), HOUR)
        as tdiffHoursEff,

        case
          when numer.eventId is not null then True
          else False
          end
        as numerEverComplete

    from dangling_denom_transformed

    order by denom.eventId, denom.eventCreatedAt, numer.eventCreatedAt

),

final as (

    select
        denom.patientId as patientId,
        denom.eventId as eventIdD,
        denom.eventCreatedAt as eventCreatedAtD,
        numer.eventId as eventIdN,
        numer.eventCreatedAt as eventCreatedAtN,
        tdiffHours,

        {% for daydelta in daydelta_list %}
        case
          when tdiffHours between 0 and {{ daydelta }}*24 then True
          else False
          end
        as {{numerator~'Within'~daydelta~'Days'~denominator}},

        {% endfor %}

        numerEverComplete

    from full_output

)

select * from final

{% endmacro %}
