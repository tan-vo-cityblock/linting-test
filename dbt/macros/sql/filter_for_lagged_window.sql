{% macro filter_for_lagged_window(
    later_date="current_date",
    earlier_date="monthFrom",
    date_part="month",
    min_value=3,
    max_value=14
  )
%}

  date_diff({{ later_date }}, {{ earlier_date }}, {{ date_part }}) between {{ min_value }} and {{ max_value }}

{% endmacro %}
