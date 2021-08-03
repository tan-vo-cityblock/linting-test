
{{ config(materialized = 'table', tags = ['payer_list']) }}

{%- set payer_list = var('payer_list') -%}

{% for payer in payer_list %}

  {% if payer in ['carefirst', 'cardinal','healthy_blue'] %}

    {# do nothing; we do not produce a member table for these partners #}

  {% elif payer == 'tufts' %}

    select distinct
      memberIdentifier.patientId,
      dateEffective.from as eligibilityMonth,
      '{{ payer }}' as partnerName

    from {{ source(payer, 'Member') }}
    
    where memberIdentifier.patientId is not null

  {% else %}  

    select distinct
      base.identifier.patientId,
      eligibility.date.from as eligibilityMonth,
      '{{ payer }}' as partnerName
    
    from {{ source(payer, 'Member') }} base
    
    left join unnest(eligibilities) as eligibility
    
    where base.identifier.patientId is not null

    union all

  {% endif %}

{% endfor %}
