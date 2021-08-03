with ehr_appointments as (

    select * from {{ ref('abs_elation_appointment_statuses') }}

    union all

    select * from {{ ref('abs_epic_appointment_statuses') }}
)

select 
    * except(appointmentStatus),

    -- appointment modality logic (modality field is created here)
    case 
        when lower(appointmentType) like '%async%' then 'Async Visit Note'
        when lower(appointmentType) like '%home%' then 'House Call Visit Note'
        when lower(appointmentType) like '%hub%' then 'Office Visit Note'
        when lower(appointmentType) like '%televideo%' then 'Telemedicine Note'
        when lower(appointmentType) like '%telephon%' then 'Telephone Visit Note'
        when lower(appointmentType) like '%telehealth%' then 'Telephone Visit Note' -- added to capture legacy visits
        when lower(appointmentType) like '%virtual%' then 'Telemedicine Note' -- added to capture epic virtual visits    
        when lower(appointmentType) like '%office%' then 'Office Visit Note' -- added to capture legacy visits
        when lower(appointmentType) like '%video%' then 'Telemedicine Note' -- added to capture legacy visits
        when lower(appointmentType) like '%app%' then 'App Visit Note'
        -- for remaining epic visit reasons, categorizing as other F2F or other Non-F2F
        when (
            lower(appointmentType) like '%education%' or 
            lower(appointmentType) like '%procedure%' or 
            lower(appointmentType) like '%initial%' or 
            lower(appointmentType) like '%anticoag%' or 
            lower(appointmentType) like '%prenatal%' or 
            lower(appointmentType) like '%postpartum%' or 
            lower(appointmentType) like '%cardio%' or 
            lower(appointmentType) like '%follow%'
        ) then 'Other F2F Visit' 
        when (
            lower(appointmentType) like '%orders only%' or 
            lower(appointmentType) like '%refill%' or 
            lower(appointmentType) like '%documentation%' or 
            lower(appointmentType) like '%outreach%' or 
            lower(appointmentType) like '%clinical support%' or 
            lower(appointmentType) like '%healthix%' or 
            lower(appointmentType) like '%advantasure%' or 
            lower(appointmentType) like '%travel%' or 
            lower(appointmentType) like '%lab%'
        ) then 'Other Non-F2F Visit' 
        -- for remaining elation visit types, categorizing as other        
        else 'Other' end as appointmentModality,

    -- appointment status normalization/cleanup
    case 
        when lower(appointmentStatus) in ('notseen', 'no show') then 'noShowed'
        when lower(appointmentStatus) = 'canceled' then 'cancelled'
        when lower(appointmentStatus) in ('checkedout', 'completed', 'billed') then 'completed' -- tbd whether billed should be treated the same as completed?
        else lower(appointmentStatus)
        end as appointmentStatus,

from ehr_appointments
