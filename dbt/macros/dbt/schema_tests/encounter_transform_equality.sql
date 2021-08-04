{% macro test_encounter_transform_equality(model) %}

{% set compare_model = kwargs.get('compare_model', kwargs.get('arg')) %}

{#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
{%- if not execute -%}
    {{ return('') }}
{% endif %}

WITH concat_b AS (SELECT
                    CONCAT(
                      TO_JSON_STRING(messageId),
                      TO_JSON_STRING(patient),
                      LOWER(TO_JSON_STRING(encounter.type)),
                      LOWER(TO_JSON_STRING(encounter.dateTime)),
                      LOWER(TO_JSON_STRING(encounter.endDateTime)),
                      LOWER(TO_JSON_STRING((SELECT AS STRUCT firstName, lastName FROM UNNEST(encounter.providers))))
                      ) AS b FROM {{ model }}),


        concat_a AS (SELECT
                    CONCAT(
                      TO_JSON_STRING(messageId),
                      TO_JSON_STRING(patient),
                      LOWER(TO_JSON_STRING(encounter.Type)),
                      LOWER(TO_JSON_STRING(encounter.DateTime)),
                      LOWER(TO_JSON_STRING(encounter.EndDateTime)),
                      LOWER(TO_JSON_STRING((SELECT AS STRUCT FirstName, LastName FROM UNNEST(encounter.Providers))))
                      ) AS a FROM {{ compare_model }}),



grouped_a AS (SELECT a AS aa, COUNT(*) AS count1
FROM concat_a
GROUP BY aa),

grouped_b AS (SELECT b AS bb, COUNT(*) AS count2
FROM concat_b
GROUP BY bb),


final AS (SELECT * FROM grouped_a
LEFT JOIN grouped_b
ON grouped_a.aa = grouped_b.bb
WHERE grouped_b.bb IS NULL)

SELECT COUNT(*) FROM final

{% endmacro %}