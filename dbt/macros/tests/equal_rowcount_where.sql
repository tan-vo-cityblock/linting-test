{# Tests that model row count is equal to comparison model, with condition applied #}
{# Modifies dbt_utils.equal_rowcount, adding compare_model_condition parameter #}
{% macro test_equal_rowcount_where(model) %}

{% set compare_model = kwargs.get('compare_model', kwargs.get('arg')) %}
{% set compare_model_condition = kwargs.get('compare_model_condition', "true") %}

{#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
{%- if not execute -%}
    {{ return('') }}
{% endif %}

with a as (

    select count(*) as count_a from {{ model }}

),
b as (

    select count(*) as count_b from {{ compare_model }} where {{ compare_model_condition }}

),
final as (

    select abs(
            (select count_a from a) -
            (select count_b from b)
            )
        as diff_count

)

select diff_count from final

{% endmacro %}