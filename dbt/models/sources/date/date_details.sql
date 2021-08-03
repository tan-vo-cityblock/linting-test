
with date_spine as (

  select
    dateDay

  from UNNEST(
    GENERATE_DATE_ARRAY(
      DATE('2017-01-01'),
      date_add(CURRENT_DATE(), interval 1 year),
      INTERVAL 1 DAY))
    AS dateDay

),

extracted as (

    select

        dateDay,

        format_date('%A', dateDay) AS dayName,

        extract(month from dateDay) AS month,
        extract(year from dateDay) AS year,
        extract(quarter from dateDay) AS quarter,

        extract(dayofweek from dateDay) AS dayOfWeek,
        extract(day from dateDay) AS dayOfMonth,

        DATE_TRUNC(dateDay, MONTH) AS firstDayOfMonth,
        DATE_SUB(DATE_TRUNC(DATE_ADD(dateDay, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY) AS lastDayOfMonth,

        DATE_TRUNC(dateDay, YEAR) AS firstDayOfYear,
        DATE_SUB(DATE_TRUNC(DATE_ADD(dateDay, INTERVAL 1 YEAR), YEAR), INTERVAL 1 DAY) AS lastDayOfYear

    from date_spine

),

calculated as (

    select

        *,

        concat(cast(year as string), '_', format("%02d", month)) as yearMonth,

        ROW_NUMBER() OVER (PARTITION BY year, quarter ORDER BY dateDay) AS dayOfQuarter,
        ROW_NUMBER() OVER (PARTITION BY year ORDER BY dateDay) AS dayOfYear,

        case
          when month = 1 then 'January'
          when month = 2 then 'February'
          when month = 3 then 'March'
          when month = 4 then 'April'
          when month = 5 then 'May'
          when month = 6 then 'June'
          when month = 7 then 'July'
          when month = 8 then 'August'
          when month = 9 then 'September'
          when month = 10 then 'October'
          when month = 11 then 'November'
          when month = 12 then 'December'
          else null
          end
        as monthName,

        FIRST_VALUE(dateDay) OVER (PARTITION BY year, quarter ORDER BY dateDay) AS firstDayOfQuarter,
        LAST_VALUE(dateDay) OVER (PARTITION BY year, quarter ORDER BY dateDay) AS lastDayOfQuarter,

        concat(cast(year as string), '_', 'Q', cast(quarter as string)) AS quarterName,
        concat(cast(year as string), '_', 'Q', cast(quarter as string)) AS yearQuarter,

        CASE
          WHEN dateDay = firstDayOfMonth THEN TRUE
          ELSE FALSE
          END
        AS isFirstDayOfMonth,

        CASE
          WHEN dateDay = lastDayOfMonth THEN TRUE
          ELSE FALSE
          END
        AS isLastDayOfMonth

    from extracted

)

select * from calculated
