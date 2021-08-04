
{% set user_list = ['createdBy', 'completedBy', 'assignedTo'] %}

with

{% for user in user_list %}

{{ user }} as (

  select 
    goalGroupId, 
    groupId as taskGroupId, 
    {{ user ~ 'Id' }} as userId
  
  from {{ source('commons', 'task') }}
  
  where deletedAt is null

),

{% endfor %}

final as (

  {% for user in user_list %}

    select * from {{ user }}

    {% if not loop.last %} 

      union distinct

    {% endif %}

  {% endfor %}

)

select * from final
