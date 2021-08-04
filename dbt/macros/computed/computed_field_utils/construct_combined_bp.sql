
{% macro construct_combined_bp(
  
  vitals_code,
  vitals_code_2,
  severe_min=None,
  severe_min_2=None,
  moderate_min=None,
  moderate_min_2=None,
  mild_min=None,
  mild_min_2=None,
  stable_cpt=None,
  stable_cpt_1=None,
  stable_cpt_2=None,
  period="1 year",
  vitals_table=ref('vitals'),
  cpt_table=ref('abs_procedures')

	)
%}

with 

  {% set vitals_cte_dict = {
      "vitals": vitals_code, 
      "vitals_2": vitals_code_2
      }
  %}

  {% for cte_name, code in vitals_cte_dict.items() %}

    {{ cte_name }} as (

    select patientId, timestamp, max(value) as value
    from {{ vitals_table }}
    where 
    	code = '{{ code }}' and 
    	date(timestamp) > date_sub(current_date, interval {{ period }})
    group by patientId, timestamp

   ),

  {% endfor %}
  
combined_vitals as ( 

  select
    patientId, 
    timestamp,

    {% if severe_min and severe_min_2 and moderate_min and moderate_min_2 and mild_min and mild_min_2 %}

      case
        when vitals.value >= {{ severe_min }} or vitals_2.value >= {{ severe_min_2 }} then 4
        when vitals.value >= {{ moderate_min }} or vitals_2.value >= {{ moderate_min_2 }} then 3
        when vitals.value >= {{ mild_min }} or vitals_2.value >= {{ mild_min_2 }} then 2
        else 1 
      end as acuity

    {% else %}

      1 as acuity

    {% endif %}

  from vitals
  inner join vitals_2
  using (patientId, timestamp)

),

{% if stable_cpt and stable_cpt_1 and stable_cpt_2 %}

  {% set cte_dict = {
      "stable_procs": stable_cpt, 
      "stable_procs_1": stable_cpt_1,
      "stable_procs_2": stable_cpt_2
      }
  %}

  {% for cte_name, cpt_code in cte_dict.items() %}

    {{ cte_name }} as (

      select distinct
        memberIdentifier as patientId,
        timestamp(serviceDateFrom) as timestamp,
        1 as acuity
      from {{ cpt_table }}
      where 
        memberIdentifierField = 'patientId' and
        procedureCode = '{{ cpt_code }}' and
        serviceDateFrom > date_sub(current_date, interval {{ period }} )

     ),

  {% endfor %}

  stable_combined_procs as (

    select patientId, timestamp, acuity
    from stable_procs_1
    inner join stable_procs_2
    using (patientId, timestamp, acuity)

  ),

  all_stable_procs as (

    select * from stable_procs 
    union distinct
    select * from stable_combined_procs

  ),

{% endif %}

all_vitals as (

  select * from combined_vitals

  {% if stable_cpt and stable_cpt_1 and stable_cpt_2 %}

    union distinct 
    select * from all_stable_procs
  
  {% endif %}

),

{% endmacro %}
