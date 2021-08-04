

with base as (

    select
        'facility' as claimType,
        claimId,
        partnerclaimid,
        linenumber,
        lineId,
        partner,
        surrogateId,
        surrogateProject,
        surrogateDataset,
        surrogateTable

    from {{ ref('abs_facility_flat') }}

    union all

    select
        'professional' as claimType,
        claimId,
        partnerclaimid,
        linenumber,
        lineId,
        partner,
        surrogateId,
        surrogateProject,
        surrogateDataset,
        surrogateTable

    from {{ ref('abs_professional_flat') }}

),

capitation_info as (

    select
        patientid
        , member_id
        , claim_id
        , sv_line

    from {{ ref('ftr_claim_capitation_intermediate') }}
    where SNF_Cases = 1
          or SNF_PROF = 1
          or IRF_Cases = 1
          or IRF_PROF = 1
          or LTAC_Cases = 1
          or LTAC_PROF = 1
          or HH_Cases = 1
          or HDME_Cases = 1
          or Chiro_2020_Cases = 1
          or Chiro_Prior_Cases = 1
          or BH_Facility = 1
          or BH_Prof = 1
          or PayType2 = 'CAP'
),

emblem_info as (

    select
        identifier.surrogateId,
        claim.medicalcenternumber,
        claim.FFSCAPIND,
        claim.responsibleindicator,
        claim.SupergroupID,
        claim.CL_DATA_SRC,
        claim.CARECORERESPONSIBLEAMT

    from {{ source('emblem_silver', 'professional') }}

    union all

    select
        identifier.surrogateId,
        claim.medicalcenternumber,
        claim.FFSCAPIND,
        claim.responsibleindicator,
        claim.SupergroupID,
        claim.CL_DATA_SRC,
        claim.CARECORERESPONSIBLEAMT

    from {{ source('emblem_silver', 'facility') }}

),


joined as (

    select
        base.*,
        case 
          when capitation_info.claim_id is not null then 1
          else 0
          end 
        as Capitated,
        emblem_info.* except(surrogateId)

    from base

    left join capitation_info
      on base.partnerclaimid = capitation_info.claim_id
      and base.linenumber = capitation_info.sv_line

    left join emblem_info
      on base.surrogateId = emblem_info.surrogateId

),

flagged as (

    select distinct
        claimId,
        lineId,

        case
          when partner = 'emblem'
            and substr(medicalcenternumber, 0, 2) in ('02', '03', '04', '05', '06', '07', '08', '09')
            and FFSCAPIND = 'C'
            and responsibleindicator = 'G'
            and (SupergroupID is null or Supergroupid <> 'FD')
            and claimType = 'professional'
            then 'acpny'
          else null
          end
        as capitatedParty,

        case
          when Capitated = 1 then true 
          when partner != 'emblem' then null
          else false
          end
        as capitatedFlag

    from joined

)

select * from flagged
