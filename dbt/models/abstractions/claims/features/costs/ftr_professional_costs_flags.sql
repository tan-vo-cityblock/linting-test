

{{
  config(
    materialized='table'
  )
}}


with base as (

  select
      base.claimId,
      base.lineId,
      base.serviceQuantity * cast(cu_partb_drugs.partb_drug_cost as NUMERIC) as totalDrugCost

    from {{ ref('abs_professional_flat') }} as base

    left join {{ source('claims_mappings', 'cu_partb_drugs') }} as cu_partb_drugs
      on base.procedureCode = cu_partb_drugs.proc_code

),

flags as (

    select
        claimId,
        lineId,

        case
          when sum(totalDrugCost) over (PARTITION BY claimId) >= 500 then true
          else false
          end
        as highDrugCostFlag

    from base

)

select * from flags