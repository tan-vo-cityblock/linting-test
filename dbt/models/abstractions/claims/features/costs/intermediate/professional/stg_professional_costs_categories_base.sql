
with base as (

    select distinct
        base.claimId,
        base.lineId,
        base.placeOfService,
        base.providerServicingSpecialty,
        base.dateFrom,

        conditions.* except (claimId, lineId),
        services.* except (claimId, lineId),
        overlap.* except (claimId, lineId),
        costs.* except (claimId, lineId),

        ifnull(cast(cu_cpt_hcpcs.outpt_hierarchy as INT64), 8) as outpatientCptHierarchy,
        cu_cpt_hcpcs_hh.sub_class as homeHealthSubClass,

    from {{ ref('abs_professional_flat') }} as base

    left join {{ source('claims_mappings', 'cu_cpt_hcpcs') }} as cu_cpt_hcpcs
      on base.procedureCode = cu_cpt_hcpcs.cpt_hcpcs_code
    
    left join {{ source('claims_mappings', 'cu_cpt_hcpcs_health_home') }} as cu_cpt_hcpcs_hh
      on base.procedureCode = cu_cpt_hcpcs_hh.cpt_hcpcs_code

    left join {{ ref('ftr_professional_conditions_flags') }} as conditions
      on base.claimId = conditions.claimId
        and base.lineId = conditions.lineId

    left join {{ ref('ftr_professional_services_flags') }} as services
      on base.claimId = services.claimId
        and base.lineId = services.lineId

    left join {{ ref('ftr_professional_costs_flags') }} as costs
      on base.claimId = costs.claimId
        and base.lineId = costs.lineId

    left join {{ ref('ftr_professional_inpatient_overlap_flags') }} as overlap
      on base.claimId = overlap.claimId
        and base.lineId = overlap.lineId
),

mapped as (

    select
        * except (outpatientCptHierarchy),

        case
          when (highDrugCostFlag = true and outpatientCptHierarchy = 8) then 7
          else outpatientCptHierarchy
          end
        as outpatientCptHierarchy

    from base

),

grouped as (

    select
        *,

        outpatientCptHierarchy as professionalLineHierarchy,

        min(outpatientCptHierarchy)
          over (PARTITION BY claimId)
        as professionalHierarchy,

        min(outpatientCptHierarchy)
          over (PARTITION BY claimId, placeOfService, dateFrom)
        as professionalLineGroupHierarchy,

        -- home health hierarchy of subcategories
        min(
          case 
            when homeHealthSubClass = 'Infusions' then 1
            when homeHealthSubClass = 'VNS' then 2
            when homeHealthSubClass = 'PT-OT-ST' then 3
            when homeHealthSubClass like 'PCA%' then 4
            else 5 end) 
            over (PARTITION BY claimId)
          as professionalHomeHealthHierarchy,

    from mapped

),


with_category as (

    select
        *,
        'professional' as costCategory

    from grouped

)

select * from with_category
