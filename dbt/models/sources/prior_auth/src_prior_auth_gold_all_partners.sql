{% for partner in ['emblem', 'cci'] %}

	select *,
		parse_date('%Y%m%d', _table_suffix) as receivedDate

	from {{ source(partner, 'PriorAuthorization_*') }}

	{% if not loop.last %} union all {% endif %}

{% endfor %}
