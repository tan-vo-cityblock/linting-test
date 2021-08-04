
----------------------------
--- Section One: Configs ---
----------------------------


--ref payer list
{{- config(tags=['payer_list']) -}}
{%- set payer_list = var('payer_list') -%}


-------------------------------------
--- Section Two: Reference Tables ---
-------------------------------------


--load member list
with pharm_claims as (

select distinct  
drug.*

from 

({% for source_name in payer_list -%}
select  distinct
identifier.id as pharmId,
memberIdentifier.partnerMemberId,
memberIdentifier.patientId,
date.filled as evidenceDate,
drug.ndc,
drug.ingredient,
drug.daysSupply
from
{{ source(source_name, 'Pharmacy') }}
{% if not loop.last -%} union all {%- endif %}
{% endfor %}
) drug

where
EXTRACT(YEAR FROM evidenceDate) between EXTRACT(YEAR FROM current_date()) - 2 and EXTRACT(YEAR FROM current_date())
and patientID is not null
)

select * from pharm_claims
