--fill in reasons closed from title sheet DO-830
with case_categories as (
select
Medicaid_ID,
inactive_date,
--per Tina, we do not get enough information to place inactive patients in the CF categories
--we go based on the 'CF member elig. list' which are general Disenrolled
case
    when lower(Reason_Why_Inactive) like '%deceased%'
      then "Death"
  else "Disenrolled" end as reasons_category
from {{ ref('care_coordination_cm_enrollment_report') }}
--want to make sure we take only those inactive from the previous month (reporting month)
where EXTRACT (YEAR from last_day(date_sub(current_date, interval 1 month), month))||extract(month from last_day(date_sub(current_date, interval 1 month), month))
= EXTRACT (YEAR from inactive_date)||extract(month from inactive_date)
and inactive_date is not null
)

select
count(distinct case when reasons_category = "Death" then Medicaid_ID else null end) as Death,
count(distinct case when reasons_category = "Disenrolled" then Medicaid_ID else null end) as Disenrolled,
count(distinct Medicaid_ID) as number_inactive_in_month
from case_categories
