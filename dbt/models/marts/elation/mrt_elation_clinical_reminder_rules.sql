{{ config(tags = ["evening"]) }}

with clin_rules as (

select distinct
*
from
{{ref('mrt_elation_clinical_rules_int')}}
),


maxdates as (

select distinct
max(runDate) as maxDate

from
clin_rules
),


final as (

select distinct
plan,
measure_code as measure_id,
additional_codes,
name,
details,
code_1,
code_1_description,
code_1_action,
code_2,
code_2_description,
code_2_action,
code_3,
code_3_description,
code_3_action,
code_4,
code_4_description,
code_4_action,
code_5,
code_5_description,
code_5_action,
code_6,
code_6_description,
code_6_action

from
clin_rules

inner join
maxdates
on
extract(date from clin_rules.rundate)  = extract(date from maxdates.maxdate)
)

select * from final