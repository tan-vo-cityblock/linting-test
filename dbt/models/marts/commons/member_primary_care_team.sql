
{% set roles = dbt_utils.get_column_values(table=ref('member_current_care_team_all'), column='userRole') %}

{% set suffix_list = ['Name', 'Email', 'Phone', 'UserId'] %}
{% set field_list = [] %}

with 

{% for role in roles %}

  {{ role }} as (

    select distinct
      patientId,
      careTeamMemberAssignedAt, 
      userName,
      userEmail,
      userPhone,
      userId,
      rank() over (partition by patientId order by careTeamMemberAssignedAt desc, userId) as rnk

    from {{ ref('member_current_care_team_all') }}

    where userRole = "{{ role }}"

    ),

  {% set temp_field_list = [] %}

  {% for suffix in suffix_list %}

    {% set field_name = 'primary' ~ role | replace("_", "") ~ suffix %}
    {% set _ = temp_field_list.append(field_name) %}

  {% endfor %}

  {% set primary_name_field = temp_field_list[0] %}
  {% set primary_email_field = temp_field_list[1] %}
  {% set primary_phone_field = temp_field_list[2] %}
  {% set primary_id_field = temp_field_list[3] %}

  {% set _ = field_list.append(primary_name_field) %}
  {% set _ = field_list.append(primary_email_field) %}
  {% set _ = field_list.append(primary_phone_field) %}
  {% set _ = field_list.append(primary_id_field) %}

  {{ role ~ '_final' }} as (

    select
      patientId,
      userName as {{ primary_name_field }}, 
      userEmail as {{ primary_email_field }},
      userPhone as {{ primary_phone_field }},
      userId as {{ primary_id_field }}
        
    from {{ role }}

    where rnk = 1

    ),

{% endfor %}

final as (

  select
    m.patientId,

    {% for field in field_list %}

      {{ field }}

      {% if not loop.last %} , {% endif %}

    {% endfor %}

  from {{ ref('src_member') }} m
  
  {% for role in roles %}

    left join  {{ role ~ '_final' }} 
    using (patientId)
    
  {% endfor %}

)

select * from final

