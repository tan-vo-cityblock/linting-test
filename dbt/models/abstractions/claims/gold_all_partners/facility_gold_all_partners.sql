
{{ config(tags = ['payer_list']) }}

{%- set payer_list = var('payer_list') -%}


{% for source_name in payer_list %}
            select * from  {{ source( source_name , 'Facility') }}
          {% if not loop.last -%} union all {%- endif %}
    {% endfor %}