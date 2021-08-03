

{% macro computed_event(slug, date, table, value='true', rule=None) %}

select
    GENERATE_UUID() as id,
    patientId,
    '{{ slug }}' as eventSlug,
    {{ value }} as eventValue,
    {{ date }} as eventAt,
    CURRENT_TIMESTAMP AS createdAt

from {{ table }}

{% endmacro %}
