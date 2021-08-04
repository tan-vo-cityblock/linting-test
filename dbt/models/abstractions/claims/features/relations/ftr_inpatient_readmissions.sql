
{{
  config(
    materialized='table'
  )
}}

with base as (

    select
      base.claimId,
      coalesce(base.commonId, base.patientId, base.partnerMemberId) as memberId,

      base.dateAdmit,
      base.dateDischarge,

      stays.stayGroup

    from {{ ref('abs_facility_flat') }} as base

    left join {{ ref('ftr_facility_stays') }} as stays
      on base.claimId = stays.claimId

    left join {{ ref('ftr_facility_costs_categories') }} as buckets
      on base.claimId = buckets.claimId

    where buckets.costCategory = 'inpatient'
      and buckets.costSubCategory = 'acute'

),

flagged as (

    {{ create_readmission_flag('base',
      startDate='dateAdmit',
      endDate='dateDischarge',
      stayGroup='stayGroup',
      memberId='memberId'
    ) }}

),

final as (

    select distinct

        base.claimId,
        flagged.readmissionFlag,
        flagged.resultsInReadmissionFlag

    from base

    left join flagged
      on base.claimId = flagged.claimId

)

select * from final
