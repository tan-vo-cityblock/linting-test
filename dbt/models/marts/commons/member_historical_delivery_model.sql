with delivery_models as (
  select
    patientId,
    currentState as careDeliveryModel,
    date(createdAt) as startDate,
    coalesce(date(deletedAt), current_date()) as endDate,
    concat(reason," - ", note) as reasonNote,
    rank() over(partition by patientId, date(createdAt) order by createdAt desc) as rnk
  from {{ source('commons', 'patient_delivery_model') }}
),

member_months as ( 
  select distinct 
    patientId,
    calendarMonth,
  from delivery_models
  cross join (select date_trunc(min(startDate),month) as startMonth, date_trunc(max(endDate),month) as endMonth from delivery_models),
  unnest(generate_date_array(startMonth, endMonth, interval 1 month)) as calendarMonth
),

result as (
  select dm.*, mm.calendarMonth
  from member_months mm
  inner join delivery_models dm 
    on mm.patientId = dm.patientId
    and mm.calendarMonth between dm.startDate and date_sub(dm.endDate, interval 1 day)
    and dm.rnk = 1
),

final as (
select 
  patientId,
  calendarMonth,
  careDeliveryModel,
  -- riskDeliveryModel = careDeliveryModel from 2 months ago, unless it was null, then use 1 month ago, if still null then use current careDeliveryModel
  coalesce( 
            lag(careDeliveryModel,2) over (partition by patientId order by calendarMonth), 
            lag(careDeliveryModel,1) over (partition by patientId order by calendarMonth), 
            careDeliveryModel
           ) as riskDeliveryModel,
  reasonNote,
  startDate,
  endDate
from result
)

select * from final
