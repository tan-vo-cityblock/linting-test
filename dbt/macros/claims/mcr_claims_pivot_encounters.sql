{% macro mcr_claims_pivot_encounters(encounter_configs, count_prefix) %}

    {% for encounter in encounter_configs %}

      count(
        distinct(
          case
            when {{ encounter['flag_prefix'] }}Flag
              then dateFrom 
          end
        )
      ) as {{ count_prefix }}{{ encounter['count_root'] }}Count,

    {% endfor %}

{% endmacro %}
