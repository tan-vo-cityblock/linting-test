{% macro assessment_score(slug,
                      assessment_slug,
                      eligible_gender=None,
                      period=None,
                      period_col='patientAnswerCreatedAt',
                      assessment_table=ref('questions_answers_all'),
                      gender_table=ref('member_info'),
                      member_table=var('cf_member_table')
                     )%}

-- identifies patients with the relevant gender

with

{% if eligible_gender %}

gender_eligible_members as (

  select patientId
  from {{ gender_table }}
  where gender = '{{ eligible_gender }}'

),

{% endif %}

-- identifies patients with the relevant assessments and scores

assessment_data as (

select
      patientId,
      "true" as value,
      array_agg(
        struct(
          patientAnswerId as id,
          'patientAnswerId' as key,
          'questions_answers_all' as model,
          string(null) as code,
          date(null) as validDate
        )) as evaluatedResource

  from {{ assessment_table }}
  where
  {% if assessment_slug %}
    (
    {% for clause in assessment_slug %}

      (
        assessmentSlug = '{{ clause[0] }}' and screeningToolScore >= {{ clause[1] }}
      )
      {% if not loop.last %}
      or --members where assessement score matches first asssessment slug and score OR second assessment slug and score
      {% endif %}

    {% endfor %}

    )
    {% endif %}

    {% if period %}

    and datetime({{ period_col }}) >= datetime_sub(current_datetime, interval {{ period }})

    {% endif %}

    group by patientId, value

),

included_members as (

  select *
  from assessment_data
  {% if eligible_gender %}
  inner join gender_eligible_members
  using (patientId)
  {% endif %}

),

cf_status as (

  {{ derive_computed_field_values(table='included_members') }}

),

final as (

    {{ computed_field(slug=slug, type='assessment_score', value='value', table='cf_status', evaluated_resource='evaluatedResource') }}

)

select * from final

{% endmacro %}
