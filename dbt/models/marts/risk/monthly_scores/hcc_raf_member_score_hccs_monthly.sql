
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

(select distinct clmyear,  month, partner, lineofbusiness, patientID, conditiontype, capturedHCC, HCCDescription, capturedCoefficient as CoeffValue, component, source  from captured)

union all

(select distinct clmyear, month, partner, lineofbusiness, patientID, 'INTERACTION' as conditiontype, 'HCC COUNT' as hcc, concat('Added ',capCount,' HCCs') as HCCDescription, coefficient, 'ADDED' as component, 'interaction' as source  from captureCount)

union all

(select distinct eligyear, month, partner, lineofbusiness, patientID, 'INTERACTION' as conditiontype, 'OREC DISABLED' as hcc, 'Originally disabled flag' as HCCDescription, coefficient, 'ADDED' as component, 'interaction' as source from orec)

union all

(select distinct clmyear, month, partner, lineofbusiness, patientID, 'INTERACTION' as conditiontype, hcc, 'HCC Interactions' as HCCDescription,coefficient, 'ADDED' as component , 'interaction' as source   from interactionvar)

),


final as (

select distinct
mm.year,
mm.month,
mm.partner,
mm.lineofbusiness,
mm.monthcount,
mm.esrdStatus,
mm.hospiceStatus,
mm.institutionalStatus,
mm.disenrolledYear,
mm.newEnrollee,
hcc.component,
hcc.conditiontype,
concat(hcc.component,' ',hcc.conditiontype) as combinedTitle,
mm.patientID,
mm.age,
mm.gender,
hcc.source,
hcc.hcccode,
hcc.HCCDescription,
concat(hcc.patientID,hcccode) as memberHCCcombo,
hcc.coefficient,
mm.Original_Reason_for_Entitlement_Code_OREC

from
mm

left join
hcc
on mm.patientID = hcc.patientID
and mm.partner = hcc.partner
and mm.lineofbusiness = hcc.lineofbusiness
and mm.year = hcc.year
and mm.month = hcc.month
)

select * from final
