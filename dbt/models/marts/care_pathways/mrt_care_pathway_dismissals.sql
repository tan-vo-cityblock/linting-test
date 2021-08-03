{%- set levels = var('supported_pathway_field_levels') -%}

with dismissals as (

  select
    id as dismissalId,
    patientId,
    pathwaySlug

  from {{ ref('abs_care_pathway_dismissals') }}

),

flattened_fields as (

  select * from {{ ref('abs_care_pathway_fields__flattened') }}

),

{% for level in levels %}

  {%- set previous_level = level - 1 -%}

  level_{{ level }}_inputs as (

    select
      patientId,

      {% if loop.first %}

        pathwaySlug,

      {% else %}

        fieldSlug as level{{ previous_level }}Input,

      {% endif %}

        evaluatedResourceModel as level{{ level }}Input

    from flattened_fields
    where
      fieldLevel = {{ previous_level }} and
      fieldType = 'boolean_evidence'

  ),

{% endfor %}

wide_inputs as (

  select
    d.dismissalId,
    d.patientId,
    i1.level1Input is not null as isCurrentSuggestion,
    d.pathwaySlug,

    {% for level in levels %}

      i{{ level }}.level{{ level }}Input,

    {% endfor %}

    coalesce(

      {% for level in levels %}

        {%- set inverse_level = loop.length + 1 - level -%}

        i{{ inverse_level }}.level{{ inverse_level }}Input {% if not loop.last %},{% endif %}

      {% endfor %}

    ) as fieldSlug

  from dismissals d

  {% for level in levels %}

    {%- set previous_level = level - 1 -%}

    left join level_{{ level }}_inputs as i{{ level }}
    on
      {% if loop.first %}

        d.patientId = i{{ level }}.patientId and
        d.pathwaySlug = i{{ level }}.pathwaySlug

      {% else %}

        i{{ previous_level }}.patientId = i{{ level }}.patientId and
        i{{ previous_level }}.level{{ previous_level }}Input = i{{ level }}.level{{ previous_level }}Input

      {% endif %}

  {% endfor %}

),

final as (

  select distinct
    {{ dbt_utils.surrogate_key(['i.dismissalId', 'i.fieldSlug', 'i.level1Input', 'f.evaluatedResourceId']) }} as id,
    i.* except (patientId, fieldSlug),
    i.fieldSlug as finalInput,
    f.evaluatedResourceId,
    f.evaluatedResourceKey,
    f.evaluatedResourceModel

  from wide_inputs i

  left join flattened_fields f
  using (patientId, fieldSlug)

)

select * from final
