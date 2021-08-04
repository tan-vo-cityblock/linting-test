{%- macro medication_evidence(slug,
        hedis_medication_list=None,
        ingredients=None,
        ndc_codes=None,
        min_ndc_fill_count=1,
        period="1 year",
        period_op=">",
        min_field='dateFilled',
        min_field_value=1
      ) -%}

with

{% if hedis_medication_list %}

  ndcs as (

    select
    distinct ndc_code as ndc

    from {{ source('hedis_codesets', 'mld_med_list_to_ndc') }}
    where medication_list in {{ list_to_sql(hedis_medication_list) }}

  ),

{% endif %}

abs_medications as (

  select
  memberIdentifier as patientId,
  ndc,
  dateFilled,
  id,
  'id' as key,
  'abs_medications' as model,
  concat(ndc, " - ", ingredient) as code,
  dateFilled as validDate,
  count(distinct dateFilled) over (partition by memberIdentifier, ndc) as fillDateCount

  from {{ ref('abs_medications') }} a

  {% if hedis_medication_list %}

  inner join ndcs
  using(ndc)

  {% endif %}

  where memberIdentifierField = 'patientId'

  {%- if period and period_op -%}

  and dateFilled {{ period_op }} date_sub(current_date, interval {{ period }})

  {% endif %}

  {%- if ingredients -%}

  and ({{ chain_or('lower(ingredient)', 'like', ingredients) }})

  {%- elif ndc_codes -%}

  and ({{ chain_or("ndc", "like", ndc_codes) }})

  {% endif %}

  {%- if min_n_fills -%}

  and count(distinct concat(ndc, dateFilled)) over (partition by memberIdentifier) >= {{ min_n_fills }}

  {% endif %}

  {%- if min_n_ndcs -%}

  and count(distinct ndc) over (partition by memberIdentifier) >= {{ min_n_ndcs }}

  {% endif %}

),

member_ndcs_meeting_fill_count as (

    select *
    from abs_medications
    where fillDateCount >= {{ min_ndc_fill_count }}

),

included_members as (

  {{ aggregate_computed_field_resources(table='member_ndcs_meeting_fill_count', min_field=min_field, min_value=min_field_value) }}

),

cf_status as (

  {{ derive_computed_field_values(table='included_members') }}

),

final as (

  {{ computed_field(slug=slug, type='medication_evidence', value='value', table='cf_status', evaluated_resource='evaluatedResource') }}

)

select *
from final

{%- endmacro -%}
