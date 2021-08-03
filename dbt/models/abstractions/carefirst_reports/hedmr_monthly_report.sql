with crosswalk as (

  select patientId,
         memberId,
         firstName,
         lastName
  from {{ ref('member') }}
  where partnerName = 'CareFirst'

),

appointments as (
  select patientId,
  --open for question, as to latest appointment or all appointments
         cast(max(startAt) as date) as appointmentDate
from {{ ref('member_appointment_statuses') }}
group by 1
),

-- ALL THE CLAIMS WHICH HAD ED VISIT for the month of oct
claims_raw as (
SELECT  f.memberIdentifier.patientId as patientId,
        f.header.date.from  as edEventStartDate,
        date_add(f.header.date.from , interval 32 day) as edEvent32DaysInterval,
        case when data.POS = '21' then true else false end as IPVisitFlag,
        case when appointmentDate between f.header.date.from  and date_add(f.header.date.from , interval 32 day) then 'YES' else 'NO' end as medicalFollowupAppointmentWithPCP

FROM {{ source('carefirst', 'Facility') }} as f
CROSS JOIN
  UNNEST(f.lines) as l
LEFT JOIN  {{ source('carefirst_silver', 'facility_*') }} sf
on sf.identifier.surrogateId = l.surrogate.id
LEFT JOIN appointments a on a.patientID = f.memberIdentifier.patientId

WHERE
  f.memberIdentifier.patientId IS NOT NULL
  and revenueCode like '045%'
  -- needs to be a 2 month lag
  and f.header.date.from between DATE_TRUNC(DATE_SUB(current_date, INTERVAL 2 MONTH), MONTH) and last_day(date_sub(current_date, interval 2 month),month)

),

outreach_connection_attempts_raw as (
    select patientId,
           max(case when modality ='phone' then replace(modality,'phone','phoneCall') else modality end) as modality,
           date(attemptedAt) as outreachDate,
           max(outcome) as status
    from {{ source('commons', 'outreach_attempt') }}
    where direction = 'outbound'
group by patientId, outreachDate

union distinct


   select patientId,
          max(case when eventType= 'phone' then replace(eventType,'phone','phoneCall') else eventType end) as modality,
          date(eventTimestamp) as outreachDate, -- followup date,
          max(status)
   from {{ ref('member_interactions') }}
   where isAttemptedConnection
 group by patientId,  outreachDate
),

--claims and follow ups combo for checking if the follow-up date(eventTimestamp date) falls between ED visit date and ED visit date+ 32 days. Only include follow up events that happen in that range because they are only interested in follow up activity that is related to the ER events

--patients_with_followups_after_EDvisit as (
final as (
  select
        ' ' as MESV_ID,
         memberId as MEM_MEDCD_NO,
         lastName as MEM_LAST_NAME,
         firstName as MEM_FIRST_NAME,
         date(outreachDate) as Date_MCO_Follow_up,
         case
         --adding 48 hours to edeventdate and not using timestamp_add as edeventstartdate is in date format
             when outreachDate between edEventStartDate and date_add(edEventStartDate, interval 2 day)
              then 'YES'
             else 'NO'
         end as MCO_Followup_within_48_hrs,
         case when modality ='phoneCall' and status in ('interested', 'success', 'consented') then 'Phone - Connected'
              when modality in ('inPersonVisitOrMeeting', 'metAtProvidersOffice', 'homeVisit') then 'Face to Face'
              when modality = 'videoCall' and status = 'success' then 'Virtual - Connected'
              when (modality = 'phoneCall' and status ='leftMessage') or status = 'leftVoicemail' then 'Voicemail'
              when status in ('noAnswer','attempt') then 'Unable to Reach'
              when modality in ('smsMessage','text') then 'Text Message'
              when modality = 'mail' then 'Letter'
              when modality is null then 'None'
              else 'Other'
          end as MCO_FollowUp_type,
          case when IPVisitFlag
              then (case when modality = 'phoneCall' then 'By Phone'
                         when modality in ('inPersonVisitOrMeeting', 'metAtProvidersOffice', 'homeVisit') then 'Face to Face'
                         when status in ('noAnswer','attempt') then 'Attempted But Unable to Reach'
                         when modality = 'email' then 'Email'
                         when outreachDate is null then 'Did not participate'
                         else 'Other'
                   end )
               else 'N/A - ED visit only'
           end as MCO_Discharge_Planning_Participation,
         "No - Already enrolled in CM" as Enrollee_Referred_to_Case_Management,
        medicalFollowupAppointmentWithPCP as Medical_FollowUP_Appointment_with_PCP
from claims_raw cr
Left join outreach_connection_attempts_raw o on o.patientId = cr.patientId
inner join crosswalk c on c.patientId = cr.patientId

where (outreachDate between edEventStartDate and edEvent32DaysInterval) or outreachDate is null

)

select distinct * from final
order by MEM_MEDCD_NO, Date_MCO_Follow_up
