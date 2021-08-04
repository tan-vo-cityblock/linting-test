
with events as (

  select
    patientVisitTypeEventId,
    patientId,
    eventAt,
    eventReceivedAt,
    memberStateCategoryAtEventReceipt

  from {{ ref('mrt_toc_events') }}

),

followups as (

  select * from {{ ref('abs_toc_followups') }}

),

{% set followup_configs =

  [

    {
      'followup_source': 'outreach_attempts',
      'followup_type': 'outreach'
    },

    {
      'followup_source': 'member_interactions',
      'followup_type': 'tend'
    },

    {
      'followup_source': 'member_interactions',
      'followup_type': 'connection'
    },

    {
      'followup_source': 'member_interactions',
      'followup_type': 'in person visit'
    },

    {
      'followup_source': 'member_interactions',
      'followup_type': 'toc connection'
    },

    {
      'followup_source': 'member_interactions',
      'followup_type': 'toc in person visit'
    },

    {
      'followup_source': 'patient_screening_tool_submission',
      'followup_type': 'screening tool'
    }

  ]

%}

{% set period_types = ['discrete', 'inclusive'] %}
{% set periods = [24, 48, 72, 96, 168] %}

{% set cte_list = [] %}

{% for followup_config in followup_configs %}

  {% set followup_source, followup_type =

      followup_config['followup_source'], followup_config['followup_type']

  %}

  {% for period_type in period_types %}

    {% for period in periods %}

      {% set cte_name =

        period_type ~ '_' ~
        period ~ '_' ~
        followup_type | replace(" ", "_")

      %}

      {% set _ = cte_list.append(cte_name) %}

      {{ cte_name }} as (

        select
          e.patientVisitTypeEventId,

          '{{ followup_source }}' as followUpSource,
          '{{ period_type }}' as followUpPeriodType,
          '{{ period }} hours' as followUpPeriod,
          '{{ followup_type }}' as followUpType,

          f.* except (patientId, followUpSource, followUpType),

          case
            when timestamp_diff(f.followUpAt, e.eventReceivedAt, hour) <= 0
              then 1
            else timestamp_diff(f.followUpAt, e.eventReceivedAt, hour)
          end as hourDiffFromReceipt,

          case
            when f.followUpId is null then null
            else row_number() over(partition by e.patientVisitTypeEventId, f.followUpType order by f.followUpAt, f.followUpId)
          end as followUpNumber,

          case
            when f.followUpId is null then null
            else row_number() over(partition by e.patientVisitTypeEventId, f.followUpType order by f.followUpAt, f.followUpId) = 1
          end as isFirstFollowUp

        from events e

        left join followups f
        on
          e.patientId = f.patientId and
          e.eventAt < f.followUpAt and
          timestamp_diff(f.followUpAt, e.eventReceivedAt, hour) <= {{ period }}

          {% if period_type == 'discrete' and period != 24 %}

            and timestamp_diff(f.followUpAt, e.eventReceivedAt, hour) > ({{ period }} - 24)

          {% endif %}

          and f.followUpSource = '{{ followup_source }}'
          and f.followUpType = '{{ followup_type }}'

      ),

    {% endfor %}

  {% endfor %}

{% endfor %}

final as (

  {% for cte in cte_list %}

    select * from {{ cte }}

    {% if not loop.last %}

      union all

    {% endif %}

  {% endfor %}

)

select * from final
