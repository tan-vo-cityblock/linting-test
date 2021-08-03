
{% macro generate_code_like_clause(icd_diagnoses=None, procedure_codes=None, use_problems=False, hccs=None, provider_specialty_codes=None) %}
{# Given a set of icd diagnoses and or procedure codes generate the like
   clause the will be used in a predicate statement #}

{% if icd_diagnoses %}
{% set diagnoses_like_clause=chain_or("diagnosisCode", "like", icd_diagnoses) %}
{% endif %}

{% if procedure_codes %}
{% set procedure_like_clause=chain_or("procedureCode", "like", procedure_codes) %}
{% endif %}

{% if icd_diagnoses and use_problems == True %}
{% set problems_like_clause=chain_or("mapTarget", "like", icd_diagnoses) %}
{% endif %}

{% if hccs %}
{% set hcc_like_clause=chain_or("hcc_v24", "like", hccs) %}
{% endif %}

{% if provider_specialty_codes %}
{% set provider_specialty_codes_like_clause=chain_or("providerSpecialty", "like", provider_specialty_codes) %}
{% endif %}

{% if icd_diagnoses and procedure_codes %}
{% set code_like_clause=diagnoses_like_clause ~ " or " ~ procedure_like_clause %}
{% elif icd_diagnoses and use_problems==False %}
{% set code_like_clause=diagnoses_like_clause %}
{% elif procedure_codes %}
{% set code_like_clause=procedure_like_clause %}
{% elif icd_diagnoses and use_problems==True %}
{% set code_like_clause=problems_like_clause %}
{% elif hccs %}
{% set code_like_clause=hcc_like_clause %}
{% elif provider_specialty_codes %}
{% set code_like_clause=provider_specialty_codes_like_clause %}
{% endif %}

{{ return(code_like_clause) }}

{% endmacro %}

{% macro clean_slug(slug) %}
{# Given a slug string, replaces hyphens with underscores #}

  {{ return(slug | replace("-", "_")) }}

{% endmacro %}

{% macro slug_to_ref(slug) %}
{# Given a slug name string, create the reference to the model #}
  {{ return(ref('cf_' ~ clean_slug(slug))) }}
{% endmacro %}


{% macro union_cte_list(cte_list, union_type='distinct') %}
{# for a list of cte's create a union of all #}

{%- for cte in cte_list -%}

	select * from {{ cte }}

	{% if not loop.last %}

	union {{ union_type }}

	 {% endif %}

{%- endfor %}

{% endmacro %}

{% macro join_cte_list(cte_list, join_only=False, type='inner') %}
{# for a list of cte's create a join of all #}

	{% if join_only == True %}

    {{ type }} join {{ cte_list[0] }}
    using (patientId)

  {% else %}

    select patientId from {{ cte_list[0] }}

  {% endif %}

  {% if cte_list|length > 1 %}

    {% for cte in cte_list[1:] %}

    	{{ type }} join {{ cte }}
    	using (patientId)

    {% endfor %}

  {% endif %}

{% endmacro %}

{% macro find_age_eligible_members(age_table, min_female_age, min_male_age, max_age) %}
{# find members meeting age criteria by gender #}

  select patientId
  from {{ age_table }}
  where
    gender = 'female' and
    date_diff(current_date, dateOfBirth, year) >= {{ min_female_age }} and
    date_diff(current_date, dateOfBirth, year) <= ({{ max_age }} + 1)
    -- include members who began year at max age

  union all

  select patientId
  from {{ age_table }}
  where
    gender = 'male' and
    date_diff(current_date, dateOfBirth, year) >= {{ min_male_age }} and
    date_diff(current_date, dateOfBirth, year) <= ({{ max_age }} + 1)
    -- include members who began year at max age

{% endmacro %}

{% macro find_members_w_field_value(slugs, fieldValueOp="=", fieldValue="true", final_cte_name="members_w_included_conditions", all_slugs=False, min_fields=1) %}
{# find members with (or without) a given value for a list of fields #}

  {% set cte_list = [] %}

	{% for slug in slugs %}

    {% set clean_slug=slug | replace("-", "_") %}
    {% set _ = cte_list.append('members_w_' ~ clean_slug) %}

      members_w_{{ clean_slug }} as (

        select patientId
        from {{ slug_to_ref(slug) }}
        where fieldValue {{ fieldValueOp }} lower('{{ fieldValue }}')

        ),

  {% endfor %}

  {{ final_cte_name }} as (

  {% if all_slugs == True %}

    {{ join_cte_list(cte_list) }}

  {% else %}

    select patientId

    from

      ({{ union_cte_list(cte_list, union_type = 'all') }})

    group by patientId

    having count(*) >= {{ min_fields }}

  {% endif %}

  ),

{% endmacro %}

{% macro find_eligible_members(age_table, min_female_age, min_male_age, max_age, condition_slugs, table, excluded_condition_slugs, member_col='patientId') %}
{# find members eligible for a given field based on age by gender and other field values #}

  with

  {% if min_female_age and min_male_age and max_age %}

  age_eligible_members as (

    {{ find_age_eligible_members(age_table, min_female_age, min_male_age, max_age) }}

  ),

  {% endif %}

  {% if condition_slugs %}

    {{ find_members_w_field_value(condition_slugs) }}

  {% endif %}

  included_members as (

    select a.{{ member_col }} as patientId
    from {{ table }} a

    {% if min_female_age and min_male_age and max_age %}

    inner join age_eligible_members b
    on a.{{ member_col }} = b.patientId

    {% endif %}

    {% if condition_slugs %}

    inner join members_w_included_conditions c
    on a.{{ member_col }} = c.patientId

    {% endif %}

    {% if member_col == 'memberIdentifier' %}

    where memberIdentifierField = 'patientId'

    {% endif %}

    group by {{ member_col }}

  ),

  {% if excluded_condition_slugs %}

    {{ find_members_w_field_value(excluded_condition_slugs, final_cte_name="members_w_excluded_conditions") }}

  {% endif %}

  eligible_members as (

    select patientId from included_members

    {% if excluded_condition_slugs %}

    except distinct
    select patientId from members_w_excluded_conditions

    {% endif %}

  ),

{% endmacro %}
