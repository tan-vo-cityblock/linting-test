
{%- macro random_int(len) -%}
  {%- for _ in range(len) -%}
    {{ range(10) | random }}
  {%- endfor -%}
{%- endmacro -%}

{%- macro unique_id(count_groups=5, group_len=6, separator='') -%}
  {%- set parts -%}
    {%- for n in range(count_groups) -%}
      {{ random_int(group_len) }}
    {%- endfor -%}
  {%- endset -%}
  {{ parts|join(separator) }}
{%- endmacro -%}
