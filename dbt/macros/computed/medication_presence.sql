
{# Common functions used by the medicaiton_presence macro #}

{% macro medication_presence_having_statement(day_min, script_min, ndc_min, provider_min, subperiod, subperiod_min, supply_min) %}

    HAVING count(distinct dateFilled) >= {{ day_min }}
      {% if script_min %}
      and COUNT(DISTINCT partnerPrescriptionNumber) >= {{ script_min }}
      {% endif %}
      {% if ndc_min %}
      and COUNT(DISTINCT ndc) >= {{ ndc_min }}
      {% endif %}
      {% if provider_min %}
      and COUNT(DISTINCT prescriberNpi) >= {{ provider_min }}
      {% endif %}
      {% if supply_min %}
      and SUM(daysSupply) >= {{ supply_min }}
      {% endif %}
      {% if subperiod and subperiod_min %}
      and COUNT(CASE
                  WHEN DATETIME(dateFilled) > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ subperiod }})
                  THEN 1 END) >= {{ subperiod_min }}
      {% endif %}

{% endmacro %}

{# full medication presence macro #}

{% macro medication_presence(slug,
             min_female_age=None,
             min_male_age=None,
             max_age=None,
             condition_slugs=None,
             excluded_condition_slugs=None,
             medication_slug=None,
             hedis_medication_list=None,
             excluded_generic_product_list=None,
             excluded_drug_ids=None,
             cf_medication_list=None,
             ndc_codes=None,
             ingredients=None,
             period="1 YEAR",
             period_op=">",
             calendar_year=False,
             day_min=1,
             script_min=None,
             ndc_min=None,
             provider_min=None,
             subperiod=None,
             subperiod_min=1,
             supply_min=None,
             age_table=ref('member_info'),
             table=ref('abs_medications'),
             member_col='memberIdentifier'
           )%}

{%- set cte_list = [] %}

{{ find_eligible_members(age_table, min_female_age, min_male_age, max_age, condition_slugs, table, excluded_condition_slugs, member_col) }}

{# if a hedis medication list list is passed, use that mapping #}
{% if hedis_medication_list %}
{% set hedis_medication_list_string=list_to_sql(hedis_medication_list) %}

{% set _ = cte_list.append('hedis_medication_list') %}

hedis_medication_list as (

    SELECT
        memberIdentifier as patientId

    FROM {{ table }} as meds

    INNER JOIN {{ source('hedis_codesets', 'mld_med_list_to_ndc') }} as h1
      ON meds.ndc = h1.ndc_code

    WHERE

      memberIdentifierField = 'patientId' and

      h1.medication_list IN {{ hedis_medication_list_string }} AND

      {% if excluded_drug_ids %}

        h1.drug_id not in {{ list_to_sql(excluded_drug_ids) }} and

      {% endif %}

      {% if excluded_generic_product_list %}

        ({{ chain_and('lower(h1.generic_product_name)', 'not like', excluded_generic_product_list) }}) AND

      {% endif %}

      {% if calendar_year == True %}

      date_trunc(dateFilled, year) {{ period_op }} date_sub(date_trunc(current_date, year), interval {{ period }})

      {% else %}

      DATETIME(dateFilled) {{ period_op }} DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ period }})

      {% endif %}

    GROUP BY memberIdentifier

    {{ medication_presence_having_statement(day_min, script_min, ndc_min, provider_min, subperiod, subperiod_min, supply_min) }}

),

{% endif %}

{% if cf_medication_list %}
{% set cf_medication_list_string=list_to_sql(cf_medication_list) %}

{% set _ = cte_list.append('cf_medication_list') %}

cf_medication_list as (

    SELECT memberIdentifier as patientId

    FROM {{ table }} as meds

    inner join {{ source('code_maps', 'ndc_to_cf_map') }} as h1
      on meds.ndc = h1.normalized_ndc

    WHERE
      memberIdentifierField = 'patientId' and
      h1.cf_slug IN {{ cf_medication_list_string }} AND
      DATETIME(dateFilled) {{ period_op }} DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ period }})

    GROUP BY memberIdentifier

    {{ medication_presence_having_statement(day_min, script_min, provider_min, supply_min) }}

),

{% endif %}

{% if ndc_codes or ingredients %}

  {% if ndc_codes %}

    {% set col_name = "ndc" %}
    {% set code_like_clause=chain_or(col_name, "like", ndc_codes) %}

  {% else %}

    {% set col_name = "ingredient" %}
    {% set code_like_clause=chain_or("lower(" ~ col_name ~ ")", "like", ingredients) %}

  {% endif %}

{% set presence_cte = col_name ~ '_presence' %}

{{ presence_cte }} as (

  SELECT
      memberIdentifier as patientId,

      CASE
        WHEN "true" IN UNNEST(
          ARRAY_AGG(
            CASE
              when {{ code_like_clause }} THEN "true"
              ELSE "false"
              END
            )
          ) THEN "true"
        ELSE "false"
        END
      AS value

    FROM {{ table }}

    where DATETIME(dateFilled) {{ period_op }} DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {{ period }})

    GROUP BY memberIdentifier

    {{ medication_presence_having_statement(day_min, script_min, ndc_min, provider_min, supply_min) }}

),

{% set members_cte = col_name ~ "_members" %}
{% set _ = cte_list.append(members_cte) %}

{{ members_cte }} as (

    SELECT
        patientId

      FROM {{ presence_cte }}

      where value = 'true'

      GROUP BY patientId

),

{% endif %}

{% if medication_slug %}
{% set _ = cte_list.append('medications') %}

  {{ find_members_w_field_value([medication_slug], final_cte_name="medications") }}

{% endif %}

members_on_medication as (

  {{ union_cte_list(cte_list) }}

),

cf_status AS (

    SELECT
        patientId,

        CASE
          WHEN patientId IN (select * from members_on_medication)
            THEN "true"
          ELSE "false"
          END
        AS value

    FROM eligible_members

    GROUP BY patientId

),

final as (

    {{ computed_field(slug=slug, type='medication_presence', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
