
{% set n = range(1,13) %}

with
captureCount as (

select *

from
{{ref('hcc_captured_count_monthly')}}
),


orec as (

{% for n in n  %}

select distinct *,

{{n}} as month

from
{{ref('hcc_orec')}}

{% if not loop.last -%} union all {%- endif %}
{% endfor %}
),


interactionvar as (

select *

from
{{ref('hcc_interaction_variable_monthly')}}
),


captured as (

{% for n in n  %}

select distinct
*,
{{n}} as month

from
{{ref('hcc_captured_current_year')}}

where
extract(month from firstCodedDate) between 1 and {{n}}

{% if not loop.last -%} union all {%- endif %}
{% endfor %}
),


demo as (

{% for n in n  %}

select distinct
* ,
{{n}} as month

from
{{ ref('hcc_demo') }}

{% if not loop.last -%} union all {%- endif %}
{% endfor %}
),


mm as (

select distinct
eligyear as year,
month,
patientID,
partner,
age,
gender,
lineofbusiness,
monthcount,
esrdStatus,
hospiceStatus,
institutionalStatus,
newEnrollee,
Original_Reason_for_Entitlement_Code_OREC,
disenrolledYear

from
demo
),


hcc as (

select *

from

(select distinct eligyear as year, month, partner, lineofbusiness, patientID, 'DEMO' as conditiontype, RA_Factor_Type_Code as hcccode, 'Demographic component of HCC score' as HCCDescription, coefficient, 'BASESCORE' as component ,'mmr' as source from demo)

union all

(select distinct clmyear,  month, partner, lineofbusiness, patientID, conditiontype, capturedHCC, HCCDescription, capturedCoefficient as CoeffValue, component, source from captured)

union all

(select distinct clmyear, month, partner, lineofbusiness, patientID, 'INTERACTION' as conditiontype, 'HCC COUNT' as hcc, concat('Added ',capCount,' HCCs') as HCCDescription, coefficient, 'ADDED' as component, 'interaction' as source from captureCount)

union all

(select distinct eligyear, month, partner, lineofbusiness, patientID, 'INTERACTION' as conditiontype, 'OREC DISABLED' as hcc, 'Originally disabled flag' as HCCDescription, coefficient, 'ADDED' as component, 'interaction' as source from orec)

union all

(select distinct clmyear, month, partner, lineofbusiness, patientID, 'INTERACTION' as conditiontype, hcc, 'HCC Interactions' as HCCDescription,coefficient, 'ADDED' as component , 'interaction' as source  from interactionvar)

),


final as (

select distinct
mm.year,
mm.month,
concat(mm.partner," ",mm.lineofbusiness) as partnerLOB,
monthcount as countMonths,
esrdStatus,
hospiceStatus,
institutionalStatus,
disenrolledYear,
mm.patientID,
Original_Reason_for_Entitlement_Code_OREC,
countHCCs,
scoreHCCs

from
mm

left join
(select distinct
year,
month,
patientID,
partner,
count(distinct concat(hcc.patientID,hcccode)) as countHCCs
from
hcc
group by
year,
month,
patientID,
partner)  hccCt
on mm.patientID = hccCt.patientID
and mm.partner = hccCt.partner
and mm.year = hccCt.year
and mm.month = hccCt.month

left join
(select distinct
year,
month,
patientID,
partner,
sum(coefficient) as scoreHCCs
from
(select distinct
year,
month,
patientID,
partner,
hcccode,
coefficient
from hcc)
group by
year,
month,
patientID,
partner ) hccSUm
on mm.patientID = hccSUm.patientID
and mm.partner = hccSUm.partner
and mm.year = hccSUm.year
and mm.month = hccSUm.month
)

select * from final
