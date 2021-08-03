
with base as (

    select
      lcfr.patientId,
      lcfr.fieldSlug as sdohIndicator,
      lcfr.fieldValue as sdohIndicatorValue,
      lcfr.createdAt,

      er.id as patientAnswerId,
      er.key as surrogateKey,
      er.model as surrogateModel

    from {{ ref('latest_computed_field_results') }} as lcfr

    left join unnest(lcfr.evaluatedResource) as er

    where fieldSlug in ('smoker-assessment','unsafe-or-unstable-housing-one-year','pregnancy-assessment','qualifies-for-financial-assistance-program',
    'homelessness-assessment','palliative-question','entitlement-gaps','exposure-to-violence','low-self-efficacy','unsafe-or-unstable-housing',
    'social-vulnerability','low-social-support','transportation-challenges','food-insecurity')

)

select *
from base
