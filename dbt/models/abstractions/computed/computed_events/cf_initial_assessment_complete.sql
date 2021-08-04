
{% set table = ref('member_commons_completion') %}


{{ computed_event(
    slug="isInitialAssessmentComplete",
    date="minInitialAssessmentAt",
    rule="minInitialAssessmentAt is not null",
    value='true',
    table=table
) }}
