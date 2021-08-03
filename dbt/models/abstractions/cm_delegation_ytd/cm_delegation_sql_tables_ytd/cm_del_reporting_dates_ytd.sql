--reporting date the last day of the month prior to when pulling the report (Pulling in Nov, reporting date = Oct 31st)
--when we want to hard code a past reporting date we use this:
--dbt run -m @cm_del_reporting_dates --vars 'cm_reporting_date: date(YYYY,MM,DD)'
{{ config(materialized = 'view') }}

With reporting as (
    Select last_day(date_sub({{var("cm_reporting_claims_date_ytd")}}, interval 1 month), month) as reporting_date
)

Select
    Reporting_date,
    extract (year from reporting_date)||'.'||lpad(cast(extract(month from date(reporting_date)) as string), 2, '0') as reporting_month_yr,
    extract (year from reporting_date)||'.'||extract (quarter from reporting_date) as reporting_quarter_yr,
    extract (quarter from reporting_date) AS reporting_quarter,
    extract (year from reporting_date) AS reporting_year,
    lpad(cast(extract(month from date(reporting_date)) as string), 2, '0') as reporting_month,
    cast(reporting_date as timestamp) as reporting_datetime,
    current_date as run_date
From reporting
