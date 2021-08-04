
{% macro lab_presence(slug, 
                      lab_codes=None, 
                      value_set_name=None,
                      code_system_name=None,
                      period="1 YEAR", 
                      calendar_year=False,
                      hedis_table=source('hedis_codesets', 'vsd_value_set_to_code'),
                      lab_table=ref('lab_results'),
                      cpt_table=ref('abs_procedures')) %}

with 

{% if value_set_name %}

codes as (

  select regexp_replace(code, r"[-]", "") as code, code_system
  from {{ hedis_table }}
  where value_set_name = '{{ value_set_name }}'

),

{% endif %}

members_w_loinc_results as (

  select memberIdentifier as patientId
  from {{ lab_table }}

  {% if value_set_name %}

  inner join codes
  on loinc = code

  {% endif %}

  where 
    memberIdentifierField = 'patientId' and

    {% if lab_codes %}

      ( {{ chain_or("loinc", "like", lab_codes) }} ) and 

    {% endif %}

    {% if calendar_year == True %}

      date_trunc(date, year) > date_sub(date_trunc(current_date, year), interval {{ period }}) and

    {% else %}

      date > date_sub(current_date, interval {{ period }}) and 

    {% endif %}

    resultNumeric > 0

  group by memberIdentifier

),

{% if value_set_name %}

members_w_cpt_results as (

  select d.memberIdentifier as patientId
  from {{ cpt_table }} d
  inner join codes c
  on d.procedureCode = c.code

    {% if code_system_name %}

      and c.code_system = '{{ code_system_name }}'

    {% endif %}

  where 
    d.memberIdentifierField = 'patientId' and

    {% if calendar_year == True %}

      date_trunc(d.serviceDateFrom, year) > date_sub(date_trunc(current_date, year), interval {{ period }}) 

    {% else %}

      d.serviceDateFrom > date_sub(current_date, interval {{ period }})  

    {% endif %}

  group by d.memberIdentifier

),

combined_table as (

  select memberIdentifier from {{ lab_table }} where memberIdentifierField = 'patientId'
  union distinct
  select memberIdentifier from {{ cpt_table }} where memberIdentifierField = 'patientId'

),

{% endif %}

all_members_w_results as (

  select patientId from members_w_loinc_results
  {% if value_set_name %}
  union distinct
  select patientId from members_w_cpt_results
  {% endif %}

),

cf_status as (

  select
    memberIdentifier as patientId,
    case
      when memberIdentifier in (select * from all_members_w_results)
        then 'true'
      else 'false'
    end as value

  {% if value_set_name %}
  from combined_table

  {% else %}
  from {{ lab_table }}
  group by memberIdentifier

  {% endif %}

),

final as (

    {{ computed_field(slug=slug, type='lab_presence', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
