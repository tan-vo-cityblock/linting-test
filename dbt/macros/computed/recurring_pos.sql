
{% macro recurring_pos(

    slug,
    pos_codes,
    min_months,
    period='6 month',
    claims_table=ref('abs_professional_flat'),
    member_table=var('cf_member_table')

  )
%}

with pos_by_month as (

  select 
    patientId,
    placeOfService,
    extract(year from dateFrom) as year, 
    extract(month from dateFrom) as month
  from {{ claims_table }}
  where 
    patientId is not null and
    placeOfService in {{ list_to_sql(pos_codes) }} and 
    dateFrom > date_sub(current_date, interval {{ period }})
  group by 1,2,3,4

),

total_months as (

  select *,
    (year - (extract(year from current_date) - 1))*12 + month as totalMo
  from pos_by_month 

),

consecutive_months as (

  select *,
    totalMo - rank() over (partition by patientId, placeOfService order by totalMo) as consecutiveMo 
  from total_months 

),

minimum_consecutive as (

  select 
    patientId, 
    placeOfService,
    consecutiveMo
  from consecutive_months 
  group by 1, 2, 3
  having count(*) >= {{ min_months }}

),

qualifying_members as (

  select patientId
  from minimum_consecutive
  group by patientId

),

cf_status as (

  select 
    patientId,

    case
      when patientId in (select * from qualifying_members)
        then 'true'
      else 'false'
    end as value

  from {{ member_table }}

),

final as (

    {{ computed_field(slug=slug, type='recurring_pos', value='value', table='cf_status') }}

)

select * from final

{% endmacro %}
