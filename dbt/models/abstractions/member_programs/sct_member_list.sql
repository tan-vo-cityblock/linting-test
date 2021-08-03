{{ config(materialized='incremental', 
	tags=['sheets']) }}


with reviewed as (

select distinct
s.*,
current_timestamp() as createdAt
from
{{source('member_programs', 'sct_member_reviewed_list')}} s

{% if is_incremental() %}

left join {{ this }} t
on
s.patientId = t.patientId 
and ifnull(s.SCTStatus,'') = ifnull(t.SCTStatus,'')
and safe.parse_date( "%m/%d/%Y",s.SCTReviewDate)  = t.SCTReviewDate

where
t.patientId is null


{% endif %}
),


reformat as (
select distinct
PatientID,
MemberFullName,
MemberName,
CurrentState,
Careteam,
Superutilizer,
MemberLosttocontact,
Membernotinterested,
Pathway,
Pod,
Review,
AssignedPHIProgram,
case when SCTReviewComplete = "TRUE" then true else false end as SCTReviewComplete,
safe.parse_date( "%m/%d/%Y",SCTReviewDate) as SCTReviewDate,
MonthofSCTReview,
SCTOwner,
Followupdate,
SCTStatus,
RiskScore,
LikelytoReadmit,
ClinicalCondtion1,
Appropriate1,
ClinicalCondition2,
Appropriate2,
ClinicalCondition3,
Appropriate3,
ClinicalCondition4,
Appropriate4,
ClinicalCondition5,
Appropriate5,
Needmedicalrecords,
Typeofmedicalrecords,
Institution,
CRRMeetAndGreet,
Referral,
createdAt

from
reviewed

where 
patientId is not null
)

select * from reformat

