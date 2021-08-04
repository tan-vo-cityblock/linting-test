{%- set three_year_fields = ['PlanPaidDollarsPerMemberMonth', 'AnnualEdVisitCount', 'AnnualInpatientAdmitCount'] -%}
{%- set fields_to_remove = ['topTwoHccsIncludeDialysis', 'pathwayFieldIndicatesDialysis', 'dialysisPaidDollarsPerMonth'] -%}

{%- for field in three_year_fields -%}

  {%- for year in ['1', '2', '3'] -%}

    {%- set _ = fields_to_remove.append(field ~ 'Year' ~ year) -%}

  {%- endfor -%}

{%- endfor -%}

with inputs as (

  select * from {{ ref('mrt_delivery_model_inputs') }}

),

flags as (

  select *,
    topTwoHccsIncludeDialysis or
    pathwayFieldIndicatesDialysis or
    dialysisPaidDollarsPerMonth > 0 as hasEvidenceOfDialysis,

    {% for field in three_year_fields %}

      {% set trend_field = 'hasPositiveThreeYear' ~ field ~ 'Trend' %}

      case
        when
          {{ field }}Year2 between {{ field }}Year1 and {{ field }}Year3 and
          {{ field }}Year2 > 0
          then 1
        else 0
      end as hasPositiveThreeYear{{ field }}Trend,

    {% endfor %}

  from inputs

),

final as (

  select
    * except {{ list_to_sql(fields_to_remove, quoted=False) }}

  from flags

)

select * from final
