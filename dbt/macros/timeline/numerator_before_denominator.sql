

{% macro numerator_before_denominator(numerator, denominator) %}

WITH denominator_data AS (
    --denominator should pull the dimensions we care about associated with the metric so that its easy to pivot
    SELECT
        memberState,
        memberAcuityDescription,
        patientId,
        eventAt,
        eventName

    FROM {{ref('scd_type_2_test')}}

    WHERE eventName = "{{ denominator }}" AND eventStatus = TRUE

),

numerator_data AS (

    SELECT
        patientId,
        eventAt,
        eventName

    FROM {{ref('scd_type_2_test')}}

    WHERE eventName = "{{ numerator }}" AND eventStatus = TRUE

),

numerator_denominator_flagged AS (

    SELECT
        timestamp_trunc(dd.eventAt, month) AS year_month,
        memberAcuityDescription,
        countif(dd.eventName = "{{ denominator}}") AS {{'num_'~denominator}},
         --parameterizable pattern for numerator vs. denominator timestamp
        countif(nd.eventAt <= dd.eventAt) AS {{'num_'~denominator~'With'~numerator }}

    from denominator_data as dd

    LEFT JOIN numerator_data as nd
      ON dd.patientId = nd.patientId
        AND nd.eventAt <= dd.eventAt

    GROUP BY 1,2

    ORDER BY 1 DESC

)

--above sql block is actually wrong, need to split out into separate aggregates then join back on

SELECT *
FROM
numerator_denominator_flagged

{% endmacro %}
