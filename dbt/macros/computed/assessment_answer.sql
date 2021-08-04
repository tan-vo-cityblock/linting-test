{%- macro assessment_answer(slug,
      assessment_slug=None,
      scores=False,
      excluded_answer_slugs=None,
      question_slug=None,
      question_slug_w_answer=None,
      period="2 year",
      period_op=">=",
      min_answers=None,
      question_answer_table=ref('questions_answers_current'),
      member_table=var('cf_member_table')
    ) -%}

with members_w_assessment as (

  select
  patientId

  from  {{ question_answer_table }}
  where

  {% if question_slug_w_answer %}
  {% for clause in question_slug_w_answer %}

  (questionSlug = '{{ clause[0] }}' and answerSlug in {{ list_to_sql(clause[1]) }}) and

  {% if not loop.last %} or
  {% endif %}

  {% endfor %}
  {% endif %}

  {% if scores == True %}
  {% for clause in assessment_slug %}

  (assessmentSlug = '{{ clause[0] }}' and screeningToolScore >= {{ clause[1] }}) and

  {% endfor %}
  {% endif %}

  {% if question_slug %}
  questionSlug = '{{ question_slug }}' and
  {% endif %}

  {% if assessment_slug and scores == False %}
  assessmentSlug = '{{ assessment_slug }}' and
  {% endif %}

  {% if excluded_answer_slugs %}
  answerSlug not in {{ list_to_sql(excluded_answer_slugs) }} and
  {% endif %}

  date(submissionCompletedAt) {{ period_op }} date_sub(current_date, interval {{ period }})
  group by patientId

  {% if min_answers %}
  having count(*) >= {{ min_answers }}
  {% endif %}

),

evidence as (

  select
  a.patientId,
  qa.patientAnswerId as id,
  'patientAnswerId' as key,
  'questions_answers_current' as model,
  concat(qa.riskAreaGroupSlug, qa.questionSlug," - ", qa.answerSlug) as code,
  cast(qa.patientAnswerCreatedAt as string) as validDate

  from members_w_assessment a
  left join {{ question_answer_table }} qa
  using (patientId)

),

cf_status as (

  select
    m.patientId,
    case
      when mmc.patientId is not null
      then 'true'
      else 'false'
    end as value
  from {{ member_table }} m
  left join evidence mmc
  using (patientId)

),

final as (

  {{ computed_field(slug=slug, type='assessment_answer', value='value', table='cf_status') }}

)

select *
from final

{% endmacro %}
