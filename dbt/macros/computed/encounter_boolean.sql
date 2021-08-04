

{% macro encounter_boolean(slug, 
                           ed_visits=None,
                           acs_ed_visits=None,
                           acs_ip_admits=None,
                           bh_ip_admits=None,
                           unplanned_admits=None,
                           ip_admits=None,
                           min_day_count=None,
                           period="1 year", 
                           encounters_table=ref('abs_encounters'),
                           member_table=var('cf_member_table')
                          )%}

with members_meeting_criteria as (

  select patientId
  from {{ encounters_table }}
  where
    date > date_sub(current_date, interval {{ period }})

    {% if ed_visits == True %}
    
      and ed is true

    {% endif %}

    {% if acs_ed_visits == True %}
    
      and acsEd is true

    {% endif %}

    {% if acs_ip_admits == True and bh_ip_admits == True and unplanned_admits == True %}
    
      and (acsInpatient or bhInpatient or resultsInInpatient)

    {% elif acs_ip_admits == True and unplanned_admits == True %}
    
      and (acsInpatient is true or resultsInInpatient is true)

    {% elif bh_ip_admits == True and ip_admits == True %}
    
      and (bhInpatient is true or inpatient is true)

    {% elif unplanned_admits == True %}
    
      and resultsInInpatient is true

    {% endif %}

  group by patientId

  {% if min_day_count %}

    having count(distinct date) >= {{ min_day_count }}

  {% endif %}

),

cf_status as (

  select 
    m.patientId,
    case
      when mmc.patientId is not null 
        then 'true'
      else 'false'
    end as value
  from {{ member_table }} m
  left join members_meeting_criteria mmc
  using (patientId)

),

final as (

    {{ computed_field(slug=slug, type='encounter_boolean', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
