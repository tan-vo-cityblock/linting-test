{% macro get_columns_superset(tables, exclude=none) -%}

    {%- set exclude = exclude if exclude is not none else [] %}
    {%- set table_columns = {} %}
    {%- set column_superset = {} %}

    {%- for table in tables -%}

        {%- set _ = table_columns.update({table: []}) %}
        {%- set cols = adapter.get_columns_in_relation(table) %}
        {%- for col in cols -%}

        {%- if col.column not in exclude %}

            {# update the list of columns in this table #}
            {%- set _ = table_columns[table].append(col.column) %}

            {%- if col.column in column_superset -%}

                {%- set stored = column_superset[col.column] %}
                {%- if col.is_string() and stored.is_string() and col.string_size() > stored.string_size() -%}

                    {%- set _ = column_superset.update({col.column: col}) %}

                {%- endif %}

            {%- else -%}

                {%- set _ =  column_superset.update({col.column: col}) %}

            {%- endif -%}

        {%- endif -%}

        {%- endfor %}
    {%- endfor %}

    {%- set ordered_column_names = column_superset.keys() %}

    {{ return(ordered_column_names) }}

{%- endmacro %}
