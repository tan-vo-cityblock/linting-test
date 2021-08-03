
with member_interactions_user as (

  select 
    mi.*, 
    u.userName, 
    u.userRole
  from {{ ref('member_interactions') }} mi
  left join {{ ref('user') }} u
  using (userId)

  ),

{% set interactions = ['Tends', 'Connections', 'In_Person_Visits'] %}
{% set types = ['Attempted', 'Successful'] %}
{% set field_suffixes = ['At', 'Type', 'UserId', 'UserName', 'UserRole'] %}

{% set cte_list = [] %}
{% set field_list = [] %}

{% for interaction in interactions %}

  {% for type in types %}

    {% set cte_name = 'latest_' ~ type.lower() ~ '_' ~ interaction.lower() %}
    {% set _ = cte_list.append(cte_name) %}

    {% set singular_interaction = (interaction | replace("_", ""))[:-1] %}

    {% if type == 'Successful' %}
      
      {% set field_prefix = 'last' ~ singular_interaction %}

    {% else %}

      {# change "Attempted" to "Attempt" #}
      {% set field_prefix = 'last' ~ singular_interaction ~ type[:-2] %}
    
    {% endif %}

    {% set temp_field_list = [] %}

    {% for suffix in field_suffixes %}

      {% set field_name = field_prefix ~ suffix %}
      {% set _ = temp_field_list.append(field_name) %}
      {% set _ = field_list.append(field_name) %}

    {% endfor %}

    {{ cte_name }} as (

      select
        patientId,
        array_agg(
          struct(
            eventTimestamp as {{ temp_field_list[0] }}, 
            eventType as {{ temp_field_list[1] }},
            userId as {{ temp_field_list[2] }},
            userName as {{ temp_field_list[3] }},
            userRole as {{ temp_field_list[4] }}
          )
          order by eventTimestamp desc limit 1
        )[offset(0)].*
      from member_interactions_user
      where {{ 'is' ~ type ~ singular_interaction }} is true
      group by patientId

      ),

  {% endfor %}

{% endfor %}

final as (

    select
      patientId,
      
      {% for field in field_list %}
      
        {{ field }},

      {% endfor %}

    --if we are around 4 years later we should change this 9999 variable to something more obviously fake 
    --for now changing the null to an integer makes our sorting work properly in Looker
    case 
      when lastConnectionAt is null then 9999
      else timestamp_diff(current_timestamp, lastConnectionAt, day) 
    end as daysSinceLastConnection,
      
    case 
      when lastConnectionAttemptAt is null then 9999
      else timestamp_diff(current_timestamp, lastConnectionAttemptAt, day) 
    end as daysSinceLastConnectionAttempt
    
    from {{ cte_list[0] }}

    {{ join_cte_list(cte_list[1:], join_only=True, type='left') }}

)

select * from final
