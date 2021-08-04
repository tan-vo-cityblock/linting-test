
{% macro sdoh_presence(
    slug,
    required_question_answer_list=[],
    optional_question_answer_list=[],
    period=None,
    period_col='patientAnswerCreatedAt',
    table=ref('questions_answers_current'),
    member_table=var('cf_member_table')
  )
%}

{#
  Use a passed in list of tuples to determine which members ma
  the structure should be with first item in the tuple as the questionSlug
  and the second to be a list of the valid value. This then gets used in the
  where clause with an in statement
#}

with filtered_data as (

  select
      patientId,
      "true" as value,
      array_agg(
        struct(
          patientAnswerId as id,
          'patientAnswerId' as key,
          'questions_answers_current' as model,
          string(null) as code,
          date(null) as validDate
        )) as evaluatedResource

  from {{ table }}

  where

  {% if required_question_answer_list %}
    (
    {% for clause in required_question_answer_list %}

      (
        questionSlug = '{{ clause[0] }}' and answerSlug in {{ list_to_sql(clause[1]) }}
      )
      {% if not loop.last %}
      and
      {% endif %}

    {% endfor %}
    )
  {% if optional_question_answer_list %}
      and
  {% endif %}
  {% endif %}

  {# parse the optional questions and chain by or #}
  {% if optional_question_answer_list %}
    (
    {% for clause in optional_question_answer_list %}

      (
        questionSlug = '{{ clause[0] }}' and answerSlug in {{ list_to_sql(clause[1]) }}
      )
      {% if not loop.last %}
      or
      {% endif %}

    {% endfor %}
    )
  {% endif %}

  {% if period %}

    and datetime({{ period_col }}) >= datetime_sub(current_datetime, interval {{ period }})

  {% endif %}

  group by patientId, value

),

cf_status as (

  select
    mt.patientId,
    coalesce(fd.value, "false") as value,
    coalesce(
      fd.evaluatedResource,
      [struct(
        string(null) as id,
        string(null) as key,
        string(null) as model,
        string(null) as code,
        date(null) as validDate)]
    ) as evaluatedResource

  from {{ member_table }} mt

  left join filtered_data fd
    using (patientId)

),

final as (

    {{ computed_field(slug=slug, type='sdoh_presence', value='value', table='cf_status', evaluated_resource='evaluatedResource') }}

)

select * from final

{% endmacro %}
