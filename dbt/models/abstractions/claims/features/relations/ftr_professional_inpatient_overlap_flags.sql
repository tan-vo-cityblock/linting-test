
{{
  config(
    materialized='table'
  )
}}

{% set overlap_categories=['acute', 'maternity', 'psych', 'rehab'] %}

with professional_base as (

    select distinct
        claimId,
        lineId,
        commonId,

        dateFrom,
        dateTo

    from {{ ref('abs_professional_flat') }}

    where placeOfService = '21'

),

facility_base as (

  select distinct
      claimId,
      commonId,
      dateAdmit,
      dateDischarge

  from {{ ref('abs_facility_flat') }}

),

inpatient as (

    select
        fac.commonId,
        stays.stayGroup,
        cat.costSubCategory,

        min(fac.dateAdmit) as dateAdmit,
        max(fac.dateDischarge) as dateDischarge

    from facility_base as fac

    left join {{ ref('ftr_facility_stays') }} as stays
      on fac.claimId = stays.claimId

    left join {{ ref('ftr_facility_costs_categories') }} as cat
      on fac.claimId = cat.claimId

    where 
        cat.costCategory = 'inpatient' and
        cat.costSubCategory in {{ list_to_sql(overlap_categories) }}

    group by commonId, stayGroup, costSubCategory

),

{% for overlap_cat in overlap_categories %}

{{ overlap_cat }}_cte as (

    select
        base.claimId,
        base.lineId,

        {# aggregate again to account for overlapping stay periods #}
        case
          when base.dateFrom = min(inpatient.dateAdmit)
            or base.dateFrom = max(inpatient.dateDischarge) then true
          else false
          end
        as onInpatient{{ overlap_cat }}StartOrEndFlag,

        case
          when base.dateFrom > min(inpatient.dateAdmit)
            and base.dateFrom < max(inpatient.dateDischarge) then true
          else false
          end
        as duringInpatient{{ overlap_cat }}Flag

    from professional_base base

    left join inpatient
      on base.commonId = inpatient.commonId

    where base.dateFrom >= inpatient.dateAdmit
      and base.dateFrom <= inpatient.dateDischarge
      and '{{ overlap_cat }}' = inpatient.costSubCategory

    group by base.claimId, base.lineId, base.dateFrom

),
{% endfor %}

merged as (

    select
        base.claimId,
        base.lineId,

        {% for overlap_cat in overlap_categories %}
        {{ overlap_cat }}_cte.onInpatient{{ overlap_cat | title }}StartOrEndFlag,
        {{ overlap_cat }}_cte.duringInpatient{{ overlap_cat | title }}Flag{% if not loop.last %},{% endif %}
        {% endfor %}

    from professional_base base

    {% for overlap_cat in overlap_categories %}

    left join {{ overlap_cat }}_cte
      on base.claimId = {{ overlap_cat }}_cte.claimId
        and base.lineId = {{ overlap_cat }}_cte.lineId

    {% endfor %}

)

select * from merged
