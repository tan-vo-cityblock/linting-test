with question_assessment_screening_tool as (

  select * from {{ source('commons', 'builder_question_assessment_screening_tool') }}

),

mds_slugs as (

select questionSlug
from question_assessment_screening_tool
where screeningToolSlug = 'minimum-data-set'

),

final as (

  select qast.*,
    mds.questionSlug is not null as isAssociatedWithMds

  from question_assessment_screening_tool qast
  left join mds_slugs mds
  using (questionSlug)

)

select * from final
