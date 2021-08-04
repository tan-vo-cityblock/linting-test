


with hcc_raf_breakdown_member_hccs as (

select *
from
{{ref('hcc_raf_breakdown_member_hccs')}}
),

final as (
SELECT
distinct
partner,
lineofbusiness,
monthcount,
esrdStatus,
hospiceStatus,
institutionalStatus,
disenrolledYear,
newEnrollee,
component,
conditiontype,
combinedTitle,
patientID,
age,
gender,
Original_Reason_for_Entitlement_Code_OREC,
count(distinct memberHCCcombo	) as HCC_Count,
round(sum(coefficient),3) as hcc_sum

FROM
hcc_raf_breakdown_member_hccs

group by
partner,
lineofbusiness,
monthcount,
esrdStatus,
hospiceStatus,
institutionalStatus,
disenrolledYear,
newEnrollee,
component,
conditiontype,
combinedTitle,
patientID,
age,
gender,
Original_Reason_for_Entitlement_Code_OREC
)

select * from final