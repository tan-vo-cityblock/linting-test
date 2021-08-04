{% macro test_encounter_transform_view_unique(model) %}

{#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
{%- if not execute -%}
    {{ return('') }}
{% endif %}

WITH json AS (SELECT TO_JSON_STRING(patient_encounters) AS json_patient_encounters
              FROM {{ model }} AS patient_encounters),

     final AS (SELECT json_patient_encounters, COUNT(*)
               FROM json
               GROUP BY json_patient_encounters
               HAVING COUNT(*) > 1)

SELECT COUNT(*) FROM final


{% endmacro %}