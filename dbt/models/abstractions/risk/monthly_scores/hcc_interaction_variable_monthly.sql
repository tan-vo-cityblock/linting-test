{% set n = range(1,13) %}


--table import

with captured as (

select
*

from 
{{ref('hcc_interaction_variable')}}
),


final as (

{% for n in n %}

select
clmYear,
partner,
lineofbusiness,
patientID,
hcc,
coefficient,
{{n}} as month

from
captured

where
extract(month from EVIDENCEDATE) between 1 and {{n}}

group by
1,2,3,4,5,6

{% if not loop.last -%} union all {%- endif %}
{% endfor %}
)

select * from final