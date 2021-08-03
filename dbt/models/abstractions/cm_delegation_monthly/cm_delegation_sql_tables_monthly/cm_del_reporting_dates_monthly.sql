--reporting date the last day of the month prior to when pulling the report (Pulling in Nov, reporting date = Oct 31st)
--when we want to hard code a past reporting date we use this:
--dbt run -m @cm_del_reporting_dates --vars 'cm_reporting_date: date(YYYY,MM,DD)'
{{ config(materialized = 'view') }}

With reporting as (
    Select last_day(date_sub({{var("cm_reporting_claims_date_monthly")}}, interval 1 month), month) as reporting_date_last,
    date_trunc(date_sub({{var("cm_reporting_claims_date_monthly")}}, interval 1 month), month) as reporting_date_first
)

Select
    reporting_date_last,
    extract (year from reporting_date_last)||'.'||lpad(cast(extract(month from date(reporting_date_last)) as string), 2, '0') as reporting_month_yr,
    extract (year from reporting_date_last) AS reporting_year,
    lpad(cast(extract(month from date(reporting_date_last)) as string), 2, '0') as reporting_month,
    cast(reporting_date_last as timestamp) as reporting_datetime_last,
    reporting_date_first,
    cast(reporting_date_first as timestamp) as reporting_datetime_first,
    current_date as run_date
From reporting
