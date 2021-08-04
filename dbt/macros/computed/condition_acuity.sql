
{% macro condition_acuity(

  slug,
  primary_condition_slug,
  acuity_slug,
  need_test_slug,
  need_test_slug_moderate_severe=None,
  critical_slug=None,
  severe_slug=None,
  check_test_slug=None,
  check_test_value=check_test_slug,
  need_test_value=need_test_slug

  )
%}

with cf_status as (

  select
    patientId,
    case

      {% if critical_slug %}

      when patientId in (select patientId from {{ slug_to_ref(critical_slug) }} where fieldValue = 'true')
        then 'critical'

      {% endif %}

      {% if severe_slug %}

      when patientId in (select patientId from {{ slug_to_ref(severe_slug) }} where fieldValue = 'true')
        then 'severe'

      {% endif %}

      {% if check_test_slug and check_test_value %}

      when patientId in (select patientId from {{ slug_to_ref(check_test_slug) }} where fieldValue = 'true')
        then '{{ check_test_value }}'

      {% endif %}

      {% if need_test_slug_moderate_severe %}

      when 
        acuity.fieldValue in ('stable', 'mild') and
        patientId in (select patientId from {{ slug_to_ref(need_test_slug) }} where fieldValue = 'true')

        then '{{ need_test_value }}'

      when 
        acuity.fieldValue in ('moderate', 'severe') and
        patientId in (select patientId from {{ slug_to_ref(need_test_slug_moderate_severe) }} where fieldValue = 'true')
        
        then '{{ need_test_value }}'

      {% else %}

      when patientId in (select patientId from {{ slug_to_ref(need_test_slug) }} where fieldValue = 'true')
        then '{{ need_test_value }}'

      {% endif %}
      when condition.fieldValue = 'false' and acuity.fieldValue = 'stable'
        then 'no'
      else coalesce (acuity.fieldValue, 'no')
    end as value
  from {{ slug_to_ref(primary_condition_slug) }} as condition
  left join {{ slug_to_ref(acuity_slug) }} as acuity
  using (patientId)

),

final as (

  {{ computed_field(slug=slug, type='condition_acuity', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
