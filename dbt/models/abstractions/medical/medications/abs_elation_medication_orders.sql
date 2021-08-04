with 

med_orders as (
  select 
    cast(patient_id as string) as elationId,
    * except(patient_id)
  from {{ source('elation', 'med_order_latest') }}
),
  
patient_id as (
  select
    mem.id as patientId,
    trim(m.mrn) as elationId
  from {{ source('member_index', 'member') }} mem
  inner join {{ source('member_index', 'mrn') }} m
    on mem.mrnId = m.id
  where mem.deletedAt is null
    and m.name = 'elation' 
),

result as (
  select
    p.patientId,
    mo.elationId,
    u.userId,
    mo.id as prescriptionId,
    mo.creation_time as createdAt,
    mo.last_modified as updatedAt,
    mo.deletion_time as deletedAt,
    mo.type as orderType,
    mo.medication_type as drugType,
    mo.route as drugRoute,
    mo.form as drugForm,
    mo.strength as drugStrength,
    mo.qty as quantity,
    mo.auth_refills as refills,
    REGEXP_REPLACE(mo.ndc, r"[.\\'\s-]", '') as ndc,
    mo.displayed_medication_name as drugName,
    mo.directions,
    mo.start_date as drugStartDate,
    mo.pharmacy_instructions as pharmacyInstructions, 
  from med_orders mo
  left join patient_id p 
    using(elationId) 
  left join {{ ref('user') }} u 
    on mo.prescribing_user_id = u.elationUserId 
)

select * from result
