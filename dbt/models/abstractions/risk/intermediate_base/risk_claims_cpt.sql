
----------------------------
--- Section One: Configs ---
----------------------------


--ref payer list
{{- config(tags=['payer_list']) -}}
{%- set payer_list = var('payer_list') -%}


-------------------------------------
--- Section Two: Reference Tables ---
-------------------------------------


--load cpt list

with cpt_claims as (

select distinct
a.* , 
EXTRACT(YEAR FROM evidenceDate) as clmYear

from
  (select distinct 
  'Professional Claims' as claimType,
  memberIdentifier.partnerMemberId, 
  memberIdentifier.patientId, 
  un_lines.procedure.codeSet, 
  un_lines.procedure.code, 
  un_lines.date.from as evidenceDate
  from                
      ({% for source_name in payer_list -%}
      select * 
      from
      {{ source(source_name, 'Professional') }} 
      {% if not loop.last -%} union all {%- endif %}
      {% endfor %}) 
      left join unnest(lines) as un_lines

  union all

  select distinct 
  'Facility Claims' as claimType, 
  memberIdentifier.partnerMemberId,  
  memberIdentifier.patientId, 
  un_lines.procedure.codeSet, 
  un_lines.procedure.code, 
  header.date.from as evidenceDate 
  from
      ({% for source_name in payer_list -%}
      select * 
      from
      {{ source(source_name, 'Facility') }} 
      {% if not loop.last -%} union all {%- endif %}
      {% endfor %}) 
      left join unnest(lines) as un_lines 
  ) a

where
EXTRACT(YEAR FROM evidenceDate) between EXTRACT(YEAR FROM current_date()) - 2 and EXTRACT(YEAR FROM current_date())
and patientId is not null
)

select * from cpt_claims