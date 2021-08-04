{{ config(materialized = 'ephemeral') }}

{%- set types = ['Tend', 'Connection', 'In Person Visit', 'Toc Connection', 'Toc In Person Visit'] -%}

with interactions as (

  select
    progressNoteGroupId,
    isAttemptedTend,
    isSuccessfulTend,
    isAttemptedConnection,
    isSuccessfulConnection,
    isAttemptedInPersonVisit,
    isSuccessfulInPersonVisit,
    isAttemptedTocConnection,
    isSuccessfulTocConnection,
    isAttemptedTocInPersonVisit,
    isSuccessfulTocInPersonVisit

  from {{ ref('member_interactions') }}

),

classifications as (

  {% for type in types %}

    {%- set camel_type = type | replace(" ", "") -%}

      (
        
        select
          progressNoteGroupId,
          '{{ type.lower() }}' as interactionType,

          case
            when {{ 'isSuccessful' ~ camel_type }}
              then 'success'
            else 'attempt'
          end as interactionStatus

        from interactions

        where {{ 'isAttempted' ~ camel_type }}

      )       

        {% if not loop.last %}

          union all

        {% endif %}

  {% endfor %}

)

select * from classifications
