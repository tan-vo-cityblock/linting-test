
with 

{% set interactions = ['Tends', 'Connections', 'In_Person_Visits'] %}
{% set types = ['Attempted', 'Successful'] %}

{% set cte_list = [] %}
{% set field_list = [] %}

{% for interaction in interactions %}
  
  {% for type in types %}

    {% set cte_name = 'mtd_' ~ type.lower() ~ '_' ~ interaction.lower() %}
    {% set _ = cte_list.append(cte_name) %}

    {% set camel_interaction = interaction | replace("_", "") %}

    {% if type == 'Successful' %}

      {% set field_name = 'monthToDate' ~ camel_interaction %}
      {% set _ = field_list.append(field_name) %}

    {% else %}

      {% set field_name = 'monthToDate' ~ type ~ camel_interaction %}
      {% set _ = field_list.append(field_name) %}

    {% endif %}
    
   {{ cte_name }} as (

      select 
        patientId, 
        count(distinct memberInteractionKey) as {{ field_name }}

      from {{ ref('member_interactions') }}

      where 

        timestamp_trunc(eventTimestamp, month, "America/New_York") = timestamp_trunc(current_timestamp, month, "America/New_York") and

        {{ 'is' ~ type ~ camel_interaction[:-1] }} is true

      group by patientId

    ),

   {% endfor %}

{% endfor %}

final as (

  select 
    patientId,
    mca.currentMemberAcuityScore,
    mca.currentMemberAcuityDescription,
    mca.targetMonthlyTendsCurrentAcuity as monthlySuggestedTends,

    {% for field in field_list %}

      coalesce({{ field }}, 0) as {{ field }}

      {% if not loop.last %}
        ,
      {% endif %}

    {% endfor %}

  from {{ cte_list[0] }} 

  left join {{ ref('member_current_acuity') }} mca
  using (patientId)

  {{ join_cte_list(cte_list[1:], join_only=True, type='left') }}

)

select * from final
