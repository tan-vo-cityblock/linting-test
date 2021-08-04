
{% macro convert_to_timing(table) %}
{# Given a CTE, converts actionDueAt timestamp to actionTiming string #}

  select 
    patientId,
    nextStep,
    case
      when actionDueAt < current_timestamp
        then 'past-due'
      when date(actionDueAt) = current_date
        then 'today'
      when date(actionDueAt) = date_add(current_date, interval 1 day)
        then 'tomorrow'
      when date_diff(date(actionDueAt), current_date, week) = 0
        then 'this-week'
      when date_diff(date(actionDueAt), current_date, week) = 1
        then 'next-week'
      when date_diff(date(actionDueAt), current_date, month) = 0
        then 'this-month'
      when date_diff(date(actionDueAt), current_date, month) = 1
        then 'next-month'
      when date_diff(date(actionDueAt), current_date, month) > 1
        then 'after-next-month'
    end as actionTiming, 
    actionRank
  from {{ table }}

{% endmacro %}
