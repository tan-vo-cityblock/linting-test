

{% macro apply_cdps_hierarchies(table_name) %}

with base as (

    select *

    from {{ table_name }}

),

interactions as (

    select

        {% set trumping={
       'INFH':   ['AIDSH'],
       'HIVM':   ['AIDSH', 'INFH'],
       'INFM':   ['AIDSH', 'INFH', 'HIVM'],
       'INFL':   ['AIDSH', 'INFH', 'HIVM', 'INFM'],
       'CANH':   ['CANVH'],
       'CANM':   ['CANVH', 'CANH'],
       'CANL':   ['CANVH', 'CANH', 'CANM'],
       'CARM':   ['CARVH'],
       'CARL':   ['CARVH', 'CARM'],
       'CAREL':  ['CARVH', 'CARM', 'CARL'],
       'CNSM':   ['CNSH'],
       'CNSL':   ['CNSH', 'CNSM'],
       'DIA1M':  ['DIA1H'],
       'DIA2M':  ['DIA1H', 'DIA1M'],
       'DIA2L':  ['DIA1H', 'DIA1M', 'DIA2M'],
       'DDL':    ['DDM'],
       'EYEVL':  ['EYEL'],
       'GIM':    ['GIH'],
       'GIL':    ['GIH', 'GIM'],
       'HEMVH':  ['HEMEH'],
       'HEMM':   ['HEMEH', 'HEMVH'],
       'HEML':   ['HEMEH', 'HEMVH', 'HEMM'],
       'METM':   ['METH'],
       'METVL':  ['METH', 'METM'],
       'PRGINC': ['PRGCMP'],
       'PSYM':   ['PSYH'],
       'PSYML':  ['PSYH', 'PSYM'],
       'PSYL':   ['PSYH', 'PSYM', 'PSYML'],
       'SUBVL':  ['SUBL'],
       'PULH':   ['PULVH'],
       'PULM':   ['PULVH', 'PULH'],
       'PULL':   ['PULVH', 'PULH', 'PULM'],
       'RENVH':  ['RENEH'],
       'RENM':   ['RENEH', 'RENVH'],
       'RENL':   ['RENEH', 'RENVH', 'RENM'],
       'SKCL':   ['SKCM'],
       'SKCVL':  ['SKCM', 'SKCL'],
       'SKNL':   ['SKNH'],
       'SKNVL':  ['SKNH', 'SKNL']}
  %}

        {% set remapped=[] %}
        {% for key, value in trumping.items() %}
          case
            when (
              {% for trumping_key in value %}
                {{ 'cdps' ~ trumping_key }} = 1
                {% if not loop.last %}or {% endif %}
              {% endfor %}
            ) then 0
            else {{ 'cdps' ~ key }}
            end
          as {{ 'cdps' ~  key }},
          {% set _ = remapped.append('cdps' ~  key) %}
        {% endfor %}

        * except {{ list_to_sql(remapped | unique, quoted=False) }}

    from base

)

select * from interactions

{% endmacro %}
