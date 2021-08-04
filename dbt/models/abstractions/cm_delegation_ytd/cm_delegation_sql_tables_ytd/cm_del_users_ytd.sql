-- Count user role groups by partner and Create user role groups, one line per user per patientid

with usersmain as (
    SELECT
        u.id as userId,
        firstName,
        lastName,
        userRole,
        case when userRole = 'Nurse_Care_Manager' then 'yes'
         else 'no' end as has_rn_team_lead,
        u.createdAt as createdAtUser,
        case when u.deletedAt <= reporting_datetime then u.deletedAt else null end as deletedAtUser,
        ct.patientId,
        case when
        userRole in ("Physician", "Psychiatrist") then 'doctors'
        when userRole in ("Hub_RN", "Nurse_Care_Manager", "Nurse_Practitioner") then 'nurses'
        when userRole in ("Behavioral_Health_Specialist") then 'social_workers'
        when userRole in ("Community_Health_Partner", "Lead_Community_Health_Partner", "Outreach_Specialist")
            then 'non_clinical_staff'
        else 'care-team' end as userRole_category,
        partner.name as partner
    FROM {{ source('commons', 'user') }} u
    LEFT JOIN {{ source('commons', 'care_team') }} ct
        ON u.id = ct.userId AND ct.deletedAt is null
    LEFT JOIN {{ source('commons', 'user_clinic') }} clin
        ON u.id = clin.userId
    LEFT JOIN {{ source('commons', 'partner_clinic') }} parcl
        ON clin.clinicId = parcl.clinicId
    LEFT JOIN {{ source('commons', 'partner') }} partner
        ON parcl.partnerId = partner.id
--we want to make sure createdAt dates are between delegation date AND reporting date.
--Other dates act the same but if they are after the reporting date, they are null
    INNER join {{ ref('cm_del_delegation_dates_ytd') }} dd
        on date(u.createdat) >= delegation_at
        and partner.name = dd.Partner_List
        and ct.patientId = dd.patientid
    INNER JOIN {{ ref('cm_del_reporting_dates_ytd') }}
        on u.createdAt <= reporting_datetime
),

num_care_team_members as(
  select
      u.partner,
      count(distinct u.userid) as num_care_team_members
  from usersmain u
  group by 1
),

num_doctors as(
  select
    u.partner,
    count(distinct u.userid) as num_doctors
  from usersmain u
    where userRole_category = 'doctors'
  group by 1
),

num_nurses as(
  select
    u.partner,
    count(distinct u.userid) as num_nurses
  from usersmain u
    where userRole_category = 'nurses'
  group by 1
),

num_social_workers as(
  select
    u.partner,
    count(distinct u.userid) as num_social_workers
  from usersmain u
    where userRole_category = 'social_workers'
  group by 1
),

num_non_clinical_staff as(
  select
    u.partner,
    count(distinct u.userid) as num_non_clinical_staff
  from usersmain u
     where userRole_category = 'non_clinical_staff'
  group by 1
),

num_nurses_plus_non_clinical as(
  select
    u.partner,
    count(distinct u.userid) as num_nurses_plus_non_clinical
  from usersmain u
     where userRole_category in ('non_clinical_staff','nurses')
  group by 1
)

SELECT
    u.partner,
    u.userId,
    u.patientId,
    u.userRole,
    u.has_rn_team_lead,
    userRole_category,
    num_care_team_members.num_care_team_members,
    num_doctors.num_doctors,
    num_nurses.num_nurses,
    num_social_workers.num_social_workers,
    num_non_clinical_staff.num_non_clinical_staff,
    num_nurses_plus_non_clinical.num_nurses_plus_non_clinical,
    current_date as run_date
FROM usersmain u
left join num_care_team_members using (partner)
left join num_doctors using (partner)
left join num_nurses using (partner)
left join num_social_workers using (partner)
left join num_non_clinical_staff using (partner)
left join num_nurses_plus_non_clinical using (partner)
