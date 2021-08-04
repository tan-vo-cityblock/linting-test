{% set cost_classification_ref = ref('mrt_claims_member_cost_classification_totals') %}
{% set cost_classification_column = 'costClassification1' %}
{% set cost_classification_list =
    get_non_null_column_values(table=cost_classification_ref, column=cost_classification_column)
%}

with member_cost_classification_totals as (

  select * from {{ cost_classification_ref }}

),

member_paid_totals as (

  select
    patientId,
    sum(planPaidDollars) as planPaidDollars

  from member_cost_classification_totals
  group by 1

),

pivoted_cost_classifications as (

  select
    patientId,

    {{ dbt_utils.pivot(
        cost_classification_column,
        cost_classification_list,
        agg='max',
        then_value='planPaidDollars'
      )
    }},

    sum(if(({{ cost_classification_column }} = 'Acute IP'), admissionCount, 0)) as admissionCount, 
    sum(if({{ cost_classification_column }} = 'ED', edDayCount, 0)) as edDayCount

  from member_cost_classification_totals
  group by 1
  
),

renamed_classifications as (

  select
    patientId,

    {% for cost_classification in cost_classification_list %}

      `{{ cost_classification }}` as {{ cost_classification | replace(" ", "") }}PaidDollars,

    {% endfor %}

  from pivoted_cost_classifications

),

final as (

  select
    t.patientId,
    t.planPaidDollars,
    t.planPaidDollars - c.rxPaidDollars as medicalPaidDollars,
    c.rxPaidDollars,
    round(safe_divide(c.rxPaidDollars, t.planPaidDollars), 2) as rxPaidDollarsPercent,
    c.* except (patientId, rxPaidDollars)

  from member_paid_totals t

  left join renamed_classifications c
  using (patientId)

)

select * from final
