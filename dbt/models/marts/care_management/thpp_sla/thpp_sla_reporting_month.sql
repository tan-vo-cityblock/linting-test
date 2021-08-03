{{ config(materialized='view') }}


  /*
  Pulls the first and last date of the previous month for the rest of the SLA report
  To change replace date statements with desired start and end dates in YYYY-MM-DD format
  For example: 2020-03-01 as tuftsSlaReportStartDate
  */
 select  date_sub(date_trunc(current_date(), month), interval 1 month) as tuftsSlaReportStartDate,
         date_sub(date_trunc(current_date(), month), interval 1 day) as tuftsSlaReportEndDate
