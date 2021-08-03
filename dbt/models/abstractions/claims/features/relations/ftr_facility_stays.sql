
{{
  config(
    materialized='table'
  )
}}

with base as (

    select
        facility.claimId,
        coalesce(facility.commonId,
        facility.patientId) as commonId,

        facility.dateAdmit,
        facility.dateDischarge,
        facility.providerBillingId,
        facility.dischargeStatus,

        buckets.costCategory,
        buckets.costSubCategory

    from {{ ref('abs_facility_flat') }} as facility

    left join {{ ref('ftr_facility_costs_categories') }} as buckets
      on facility.claimId = buckets.claimId

),

{% set member_id_field='commonId' %}

acute as (

    {{ create_stay_groups('base',
      startDate='dateAdmit',
      endDate='dateDischarge',
      transferStatusCode='02',
      filter="costCategory = 'inpatient' and costSubCategory = 'acute'",
      memberId=member_id_field,
      flag_overlap=True
    ) }}

),

maternity as (

    {{ create_stay_groups('base',
      startDate='dateAdmit',
      endDate='dateDischarge',
      transferStatusCode='02',
      filter="costCategory = 'inpatient' and costSubCategory = 'maternity'",
      memberId=member_id_field,
      flag_overlap=True
    ) }}

),

psych as (

    {{ create_stay_groups('base',
      startDate='dateAdmit',
      endDate='dateDischarge',
      transferStatusCode='65',
      filter="costCategory = 'inpatient' and costSubCategory = 'psych'",
      memberId=member_id_field,
      flag_overlap=True
    ) }}

),

rehab as (

    {{ create_stay_groups('base',
      startDate='dateAdmit',
      endDate='dateDischarge',
      transferStatusCode='62',
      filter="costCategory = 'inpatient' and costSubCategory = 'rehab'",
      memberId=member_id_field,
      flag_overlap=True
    ) }}

),

snf as (

    {{ create_stay_groups('base',
      startDate='dateAdmit',
      endDate='dateDischarge',
      transferStatusCode='03',
      filter="costCategory = 'snf'",
      memberId=member_id_field,
      flag_overlap=True
    ) }}

),


unioned as (

    select *, 'acute' as staySetting from acute

    union all

    select * ,'maternity' as staySetting from maternity

    union all

    select *, 'psych' as staySetting from psych

    union all

    select *, 'rehab' as staySetting from rehab

    union all

    select *, 'snf' as staySetting from snf

    union all
    select distinct claimId, 'index' as stayType, claimId as stayGroup, 'ed' as staySetting
    from base where costSubCategory ='ed'

),

final as (

    select distinct
        base.claimId,
        ifnull(unioned.stayType, 'index') as stayType,
        unioned.stayGroup,
        unioned.staySetting


    from base

    left join unioned
      on base.claimId = unioned.claimId

)

select * from final


