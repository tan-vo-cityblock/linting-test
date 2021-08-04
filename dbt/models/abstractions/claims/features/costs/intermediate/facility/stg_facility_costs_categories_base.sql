
{{
  config(
    materialized='ephemeral'
  )
}}


with base as (

    select
        base.claimId,
        base.typeOfBill,

        conditions.* except (claimId),
        services.* except (claimId),
        locations.* except (claimId),
        costs.* except (claimId),

        cu_drg_codes.drg_mdc as mdc,
        cast(cu_outpt_rev_codes.cu_outpt_rev_cat_hier as INT64) as outpatientRevHierarchy,
        cast(cu_cpt_hcpcs.outpt_hierarchy as INT64) as outpatientCptHierarchy,
        cast(cu_rev_codes_room_board.inpt_hierarchy as INT64) as inpatientRevHierarchy,
        cu_outpt_rev_codes_hh.sub_class as hhRevSubClass,
        cu_cpt_hcpcs_hh.sub_class as hhProcSubClass,

    from {{ ref('abs_facility_flat') }} as base

    left join {{ source('claims_mappings', 'cu_drg_codes') }} as cu_drg_codes
      on base.drgCode = cu_drg_codes.drg_code
        and base.drgVersion = cu_drg_codes.drg_version

    left join {{ source('claims_mappings', 'cu_outpt_rev_codes') }} as cu_outpt_rev_codes
      on base.revenueCode = cu_outpt_rev_codes.rev_code

    left join {{ source('claims_mappings', 'cu_outpt_rev_codes_health_home') }} as cu_outpt_rev_codes_hh
      on base.revenueCode = cu_outpt_rev_codes_hh.rev_code    

    left join {{ source('claims_mappings', 'cu_rev_codes_room_board') }} as cu_rev_codes_room_board
      on base.revenueCode = cu_rev_codes_room_board.rev_code

    left join {{ source('claims_mappings', 'cu_cpt_hcpcs') }} as cu_cpt_hcpcs
      on base.procedureCode = cu_cpt_hcpcs.cpt_hcpcs_code

    left join {{ source('claims_mappings', 'cu_cpt_hcpcs_health_home') }} as cu_cpt_hcpcs_hh
      on base.procedureCode = cu_cpt_hcpcs_hh.cpt_hcpcs_code

    left join {{ ref('ftr_facility_conditions_flags') }} as conditions
      on base.claimId = conditions.claimId

    left join {{ ref('ftr_facility_services_flags') }} as services
      on base.claimId = services.claimId

    left join {{ ref('ftr_facility_locations_categories') }} as locations
      on base.claimId = locations.claimId

    left join {{ ref('ftr_facility_costs_flags') }} as costs
      on base.claimId = costs.claimId

),

categorized as (

    select
        *,

        case
          when inpatientRevHierarchy = 2 and mdc not in ('14', '15') then 5
          when mdc in ('14', '15') then 1
          when inpatientRevHierarchy is null then 6
          else inpatientRevHierarchy
          end
        as inpatientLineHierarchy,

        IFNULL(
          least(
            ifnull(outpatientRevHierarchy, 99),
            ifnull(outpatientCptHierarchy, 99)),
            99)
        as outpatientLineHierarchy

    from base

),

grouped as (

    select
        *,

        min(inpatientLineHierarchy)
          over (PARTITION BY claimId)
        as inpatientHierarchy,

        min(outpatientLineHierarchy)
          over (PARTITION BY claimId)
        as outpatientHierarchy,

      -- home health hierarchy of subcategories
        min(
          case 
            when hhRevSubClass = 'Infusions' or hhProcSubClass = 'Infusions' then 1
            when hhRevSubClass = 'VNS' or hhProcSubClass = 'VNS' then 2
            when hhRevSubClass = 'PT-OT-ST' or hhProcSubClass = 'PT-OT-ST' then 3
            when hhRevSubClass like 'PCA%' or hhProcSubClass like 'PCA%' then 4
            else 5 end) 
            over (PARTITION BY claimId)
          as outpatientHomeHealthHierarchy

    from categorized

),

mapped as (

    select
        * except (outpatientHierarchy),

        case
          when (highDrugCostFlag = true and outpatientHierarchy in (3, 8)) then 7
          else outpatientHierarchy
          end
        as outpatientHierarchy

    from grouped

),

with_category as (

    select

        *,

        case
          when typeOfBill like '011%' then 'inpatient'
          when (typeOfBill like '021%' and dialysisServiceFlag = false) then 'snf'
          when ((typeOfBill not like '011%' and typeOfBill not like '021%') or
               (typeOfBill like '021%' and dialysisServiceFlag = true))
               then 'outpatient'
          else null
          end
        as costCategory

    from mapped

)

select * from with_category
