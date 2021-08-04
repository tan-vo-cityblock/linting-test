{{
  config(
    materialized='table'
  ) 
}}

with abs_facility_flat as (

  select
    patientId,
    claimId,
    partner,
    dateFrom,
    dateTo,
    dateAdmit,
    dateDischarge
  from {{ ref('abs_facility_flat') }}
  where patientId is not null
),

abs_professional_flat as (

  select
    claimId,
    lineId,
    patientId,
    placeOfService,
    claimLineStatus,
    amountPlanPaid,
    dateFrom
  from {{ ref('abs_professional_flat') }}
  where patientId is not null 
),

ftr_facility_stays as (

  select
    claimId,
    cast(stayGroup as string) as stayGroup,
    staySetting
  from {{ ref('ftr_facility_stays') }}
  where stayGroup is not null
),

ftr_facility_costs_categories_ed as (

  select
    claimId,
    claimId as stayGroup
  from {{ ref('ftr_facility_costs_categories') }}
  where costSubCategory = 'ed'
),

ftr_professional_costs_categories as (

  select
    claimId,
    lineId,
    costCategory,
    costSubCategory
  from {{ ref('ftr_professional_costs_categories') }}
  where costSubCategory in ('ed','inpatient','snf','inpatientPsych','inpatientMaternity','rehab')
),

professionals_ip as (

  select
    a.claimId,
    patientId,
    dateFrom,
    a.lineId,
    a.amountPlanPaid,
    claimLineStatus,
    costCategory,
    costSubCategory,
    placeOfService 
	from abs_professional_flat a
  join ftr_professional_costs_categories b
    on a.claimId = b.claimId
      and a.lineId = b.lineId
  where costSubCategory = 'inpatient'
),

professionals_snf as (

  select
    a.claimId,
    patientId,
    dateFrom,
    a.lineId,
    a.amountPlanPaid,
    claimLineStatus,
    costCategory,
    costSubCategory,
    placeOfService 
	from abs_professional_flat a
  join ftr_professional_costs_categories b
    on a.claimId = b.claimId
      and a.lineId = b.lineId
  where costSubCategory = 'snf'
),

professionals_psych as (

  select
    a.claimId,
    patientId,
    dateFrom,
    a.lineId,
    a.amountPlanPaid,
    claimLineStatus,
    costCategory,
    costSubCategory,
    placeOfService 
	from abs_professional_flat a
  join ftr_professional_costs_categories b
    on a.claimId = b.claimId
      and a.lineId = b.lineId
  where costSubCategory = 'inpatientPsych'
),

professionals_mat as (

  select
    a.claimId,
    patientId,
    dateFrom,
    a.lineId,
    a.amountPlanPaid,
    claimLineStatus,
    costCategory,
    costSubCategory,
    placeOfService 
	from abs_professional_flat a
  join ftr_professional_costs_categories b
    on a.claimId = b.claimId
      and a.lineId = b.lineId
  where costSubCategory = 'inpatientMaternity'
),

professionals_reh as (

  select
    a.claimId,
    patientId,
    dateFrom,
    a.lineId,
    a.amountPlanPaid,
    claimLineStatus,
    costCategory,
    costSubCategory,
    placeOfService 
	from abs_professional_flat a
  join ftr_professional_costs_categories b
    on a.claimId = b.claimId
      and a.lineId = b.lineId
  where costSubCategory = 'rehab'
),

professionals_ed as (

  select
    a.claimId,
    patientId,
    dateFrom,
    a.lineId,
    a.amountPlanPaid,
    claimLineStatus,
    costCategory,
    costSubCategory,
    placeOfService
  from abs_professional_flat a
  join ftr_professional_costs_categories b
    on a.claimId = b.claimId
      and a.lineId = b.lineId
  where costSubCategory = 'ed'
),

admits as (

  select
    a.patientId, 
    partner, 
    cast(stayGroup as string) as stayGroup, 
    min(coalesce(dateAdmit, date(2100,1,1))) as admitDate, 
    array_agg(distinct b.claimId) as admitClaims, 
    max(coalesce(a.dateDischarge, date(2000,1,1))) as dischargeDate   
  from abs_facility_flat a 
  join ftr_facility_stays b
    on a.claimId = b.claimId
  where staySetting = 'acute'
  group by 1,2,3
),

admits_psych as (
  
  select
    a.patientId, 
    partner, 
    cast(stayGroup as string) as stayGroup, 
    min(coalesce(dateAdmit,DATE(2100,1,1))) as admitDate, 
    array_agg(distinct b.claimId) as admitClaims, 
    max(coalesce(a.dateDischarge,date(2000,1,1))) as dischargeDate   
  from abs_facility_flat a 
  join ftr_facility_stays b
    on a.claimId = b.claimId
  where staySetting = 'psych'
  group by 1,2,3
),

admits_snf as (
  
  select
    a.patientId, 
    partner, 
    cast(stayGroup as string) as stayGroup, 
    min(coalesce(dateAdmit, date(2100,1,1))) as admitDate, 
    array_agg(distinct b.claimId) as admitClaims, 
    max(coalesce(a.dateDischarge, date(2000,1,1))) as dischargeDate   
  from abs_facility_flat a 
  join ftr_facility_stays b
    on a.claimId = b.claimId
  where staySetting = 'snf'
  group by 1,2,3
),

admits_mat as (
  
  select
    a.patientId, 
		partner, 
		cast(stayGroup as string) as stayGroup, 
		min(coalesce(dateAdmit, date(2100,1,1))) as admitDate, 
		array_agg(distinct b.claimId) as admitClaims, 
		max(coalesce(a.dateDischarge, date(2000,1,1))) as dischargeDate   
  from abs_facility_flat a 
  join ftr_facility_stays b
    on a.claimId = b.claimId
  where staySetting ='maternity'
  group by 1,2,3
),

admits_reh as (
  
  select a.patientId, 
	   partner, 
	   cast(stayGroup as string) as stayGroup, 
	   min(coalesce(dateAdmit, date(2100,1,1))) as admitDate, 
	   array_agg(distinct b.claimId) as admitClaims, 
	   max(coalesce(a.dateDischarge, date(2000,1,1))) as dischargeDate   
  from abs_facility_flat a 
  join ftr_facility_stays b
    on a.claimId = b.claimId
  where staySetting = 'rehab'
  group by 1,2,3
),

ed_visits as (

  select
    a.patientId, 
    partner,
    a.claimId as stayGroup, 
    min(coalesce(dateFrom, date(2100,1,1))) as admitDate, 
    array_agg(distinct a.claimId) as admitClaims, 
    max(coalesce(a.dateTo, date(2000,1,1))) as dischargeDate   
  from abs_facility_flat a
  join ftr_facility_costs_categories_ed b
    on a.claimId = b.claimId
  group by 1,2,3
)

select 
  admits.*, 
  professionals_ip.dateFrom, 
  professionals_ip.claimId as professionalClaimId, 
  professionals_ip.placeOfService, 
  professionals_ip.costSubCategory, 
  professionals_ip.lineId, 
  professionals_ip.amountPlanPaid as professionalPaidAmount, 
  professionals_ip.claimLineStatus as claimLineStatusProf   
from admits
join professionals_ip 
  on admits.patientId = professionals_ip.patientId
where admitDate <= dateFrom 
  and dischargeDate >= dateFrom
  
  union all 
  
select
  admits_psych.*, 
  professionals_psych.dateFrom, 
  professionals_psych.claimId as professionalClaimId, 
  professionals_psych.placeOfService, 
  professionals_psych.costSubCategory, 
  professionals_psych.lineId, 
  professionals_psych.amountPlanPaid as professionalPaidAmount,
  professionals_psych.claimLineStatus as claimLineStatusProf   
from admits_psych 
join professionals_psych 
  on admits_psych.patientId = professionals_psych.patientId
where admitDate <= dateFrom 
  and dischargeDate >= dateFrom
  
  union all 
  
select
  admits_mat.*, 
  professionals_mat.dateFrom, 
  professionals_mat.claimId as professionalClaimId, 
  professionals_mat.placeOfService, 
  professionals_mat.costSubCategory, 
  professionals_mat.lineId, 
  professionals_mat.amountPlanPaid as professionalPaidAmount, 
  professionals_mat.claimLineStatus as claimLineStatusProf   
from admits_mat 
join professionals_mat 
  on admits_mat.patientId = professionals_mat.patientId
where admitDate <= dateFrom 
  and dischargeDate >= dateFrom
  
  union all 
  
select
  admits_snf.*, 
  professionals_snf.dateFrom, 
  professionals_snf.claimId as professionalClaimId, 
  professionals_snf.placeOfService, 
  professionals_snf.costSubCategory, 
  professionals_snf.lineId, 
  professionals_snf.amountPlanPaid as professionalPaidAmount, 
  professionals_snf.claimLineStatus as claimLineStatusProf   
from admits_snf 
join professionals_snf 
  on admits_snf.patientId = professionals_snf.patientId
where admitDate <= dateFrom 
  and dischargeDate >= dateFrom
  
  union all 
  
select
  admits_reh.*, 
  professionals_reh.dateFrom, 
  professionals_reh.claimId as professionalClaimId, 
  professionals_reh.placeOfService, 
  professionals_reh.costSubCategory, 
  professionals_reh.lineId, 
  professionals_reh.amountPlanPaid as professionalPaidAmount, 
  professionals_reh.claimLineStatus as claimLineStatusProf   
from admits_reh 
join professionals_reh 
  on admits_reh.patientId = professionals_reh.patientId
where admitDate <= dateFrom 
  and dischargeDate >= dateFrom
  
  union all
  
select
  ed_visits.*, 
  professionals_ed.dateFrom, 
  professionals_ed.claimId  as professionalClaimId, 
  professionals_ed.placeOfService, 
  professionals_ed.costSubCategory, 
  professionals_ed.lineId, 
  professionals_ed.amountPlanPaid as professionalPaidAmount, 
  professionals_ed.claimLineStatus as claimLineStatusProf   
from ed_visits 
join professionals_ed 
  on ed_visits.patientId = professionals_ed.patientId
where admitDate <= dateFrom 
  and dischargeDate >= dateFrom
