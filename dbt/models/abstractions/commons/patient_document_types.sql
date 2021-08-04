
{{ config(materialized='table') }}

{% set documentTypes = dbt_utils.get_column_values(table=source('commons', 'patient_document'), column='documentType') %}

{% set prefix_list = ['earliest', 'latest', 'has'] %}
{% set field_list = [] %}

with

subset_non_deleted as (
  select
    patientId,
    createdAt,
    documentType

  from {{ source('commons', 'patient_document') }}

  where deletedAt is null

),

{% for doc in documentTypes %}
  {% if doc is not none %}

    {{ doc }} as (

      select
        patientId,
        case when min(createdAt) is not null then true else false end hasDoc,
        min(createdAt) as earliestDocAt,
        max(createdAt) as latestDocAt

      from subset_non_deleted

      where documentType = "{{ doc }}"

      group by 1

      ),

    {% set temp_field_list = [] %}

    {% for prefix in prefix_list %}

      {% set field_name = prefix ~ doc | replace(doc, (doc[0] | upper) ~ (doc[1:])) %}

        {% if prefix is equalto('earliest') or prefix is equalto('latest') %}
          {% set field_name = field_name ~ 'At' %}
        {% endif %}

      {% set _ = temp_field_list.append(field_name) %}

    {% endfor %}

    {% set earliest_doc_at_field = temp_field_list[0] %}
    {% set latest_doc_at_field = temp_field_list[1] %}
    {% set has_field = temp_field_list[2] %}

    {% set _ = field_list.append(earliest_doc_at_field) %}
    {% set _ = field_list.append(latest_doc_at_field) %}
    {% set _ = field_list.append(has_field) %}

    {{ doc ~ '_final' }} as (

      select
        patientId,
        hasDoc as {{ has_field }},
        earliestDocAt as {{ earliest_doc_at_field }},
        latestDocAt as {{ latest_doc_at_field }}

      from {{ doc }}

      ),

  {% endif %}

{% endfor %}

final as (

  select
    m.patientId,

    {% for field in field_list %}

      {{ field }}

      {% if not loop.last %} , {% endif %}

    {% endfor %}

  from {{ ref('src_member') }} m

  {% for doc in documentTypes %}
    {% if doc is not none %}

      left join  {{ doc ~ '_final' }}
      using (patientId)

    {% endif %}

  {% endfor %}

)

select * from final
