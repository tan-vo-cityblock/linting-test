--HRA = comprehensive assessment

with demographics_assessment as (

  select
    member.medicaidId,
    mi.patientId,
    firstName,
    lastName,
    mi.dateOfBirth,
    email1 as emailaddress,
    SUBSTRING(phone1, 1, 3)||'-'||SUBSTRING(phone1, 4, 3)||'-'||SUBSTRING(phone1, 7, 4) as phonenumber,
    street1||" "||street2||", "||city||", "||state||", "||zip as address,
     --we want the earliest comprehensive assessment completed
    date(minComprehensiveAssessmentAt) as DOS
  from {{ ref('member') }} member
  left join {{ ref('member_info') }} mi
    on member.patientId = mi.patientId
    and member.medicaidId = mi.medicaidId
  left join {{ ref('member_commons_completion') }} mcc
    on member.patientId = mcc.patientId
  where partnerName = 'CareFirst'
  and minComprehensiveAssessmentAt is not null
  --we only want those who've taken the assessment from the previous month
  and date(minComprehensiveAssessmentAt) between DATE_TRUNC(DATE_SUB(current_date, INTERVAL 1 MONTH), MONTH) and last_day(date_sub(current_date, interval 1 month),month)

),

final as (select
  demographics_assessment.* except(patientid)
from demographics_assessment
where dos is not null
order by 1
)

select * from final
order by 1
