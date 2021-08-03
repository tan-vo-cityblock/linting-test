{% set encounter_configs = var('encounter_configs') %}
{% set count_prefix = 'annual' %}
{% set cte_list = [] %}

with mrt_claims_last_three_years_paid as (

  select
    patientId,
    yearSeq,
    {{ mcr_claims_pivot_encounters(encounter_configs, count_prefix) }}

  from {{ ref('mrt_claims_last_three_years_paid') }}
  where yearFrom < extract(year from current_date)
  group by 1, 2

),

{% for encounter in encounter_configs %}

  {% set count_root = encounter['count_root'] %}

  {% if count_root in ('EdVisit', 'InpatientAdmit') %}

    {% set count_name = count_prefix ~ count_root ~ 'Count' %}

    {{ count_root }} as (

      select
        patientId,

        {{ dbt_utils.pivot(
            'yearSeq',
            [1, 2, 3],
            agg='max',
            prefix=count_name ~ 'Year',
            then_value=count_name
          )
        }}

      from mrt_claims_last_three_years_paid
      group by 1

    ),

  {% endif %}

{% endfor %}

final as (

  select
    EdVisit.*,
    InpatientAdmit.* except (patientId)

  from EdVisit

  left join InpatientAdmit
  using (patientId)

)

select * from final
