
with professional_multimarket as (

    select distinct
        -- set of base columns from the flat table
        'medical' as claimType,
        'professional' as formType,
        flat.partner,
        flat.partnerSourceName,

        flat.claimId,
        flat.partnerClaimId,
        flat.lineNumber,
        flat.lineId,

        flat.procedureCode,
        STRING(NULL) as revenueCode, -- Only applies to facility claims

        flat.serviceQuantity,

        flat.partnerMemberId,
        flat.commonId,
        flat.patientId,

        flat.lineOfBusiness,
        flat.subLineOfBusiness,

        flat.providerServicingId,
        flat.providerServicingSpecialty,

        flat.providerBillingId,

        STRING(NULL) as billType, -- Only applies to facility claims
        flat.placeOfService,

        flat.dateFrom,
        flat.dateTo,

        flat.datePaid,

        cast(NULL as date) as dateAdmit, -- Only applies to facility claims
        cast(NULL as date) as dateDischarge, -- Only applies to facility claims
        STRING(NULL) as dischargeStatus, -- Only applies to facility claims

        flat.amountAllowed,
        flat.amountPlanPaid,

        STRING(NULL) as principalProcedureCode, -- missing
        flat.principalDiagnosisCode,
        flat.claimLineStatus,

        flat.surrogateId,

        STRING(NULL) as ndc, -- Only applies to pharmacy claims
        NULL as daysSupply, -- Only applies to pharmacy claims
        STRING(NULL) as brandIndicator, -- Only applies to pharmacy claims
        string(null) as npi, -- Only applies to gold pharmacy claims

        -- columns added on via joins to feature tables

        STRING(NULL) as outpatientLocation, -- Only applies to facility claims

        costs.costCategory as claimCategory,
        costs.costSubCategory as serviceCategory,
        costs.costSubCategoryDetail as serviceSubCategory,
        costs.costCategory,
        costs.costSubCategory,
        costs.costSubCategoryDetail,
        costs.costCategoryLine,
        costs.costSubCategoryLine,
        costs.costSubCategoryDetailLine,

        FALSE as acuteInpatientCategoryFlag, -- Only applies to facility claims
        FALSE as edCategoryFlag, -- Only applies to facility claims
        FALSE as obsCategoryFlag, -- Only applies to facility claims

        FALSE as readmissionFlag, -- Only applies to facility claims
        FALSE as resultsInReadmissionFlag, -- Only applies to facility claims

        FALSE as edServiceFlag, -- Only applies to facility claims
        FALSE as icuServiceFlag, -- Only applies to facility claims

        STRING(NULL) as stayType, -- Only applies to facility claims
        prof_admits.stayGroup, -- Added to prof claims
        concat(flat.patientId,'-',cast(flat.dateFrom as string) ) as patientDate,
        cast(surg_med.surgical as string) as surgical ,
        STRING(NULL) as drgCode,
        STRING(NULL) as drgCodeset,
        cast(unplanned.unplanned_admission as string) as unplanned_admission,

        provider_cat.providerCategory,
        provider_cat.primaryCareProviderFlag,
        provider_cat.surgicalProviderFlag,
        provider_cat.specialistProviderFlag,
        provider_cat.behavioralHealthProviderFlag,

        FALSE as acsInpatientFlag, -- Only applies to inpatient facility claims
        STRING(NULL) as acsInpatientTypes, -- Only applies to inpatient facility claims

        FALSE as acsEdFlag, -- Only applies to ed facility claims

        maxDateFrom 

    from {{ ref('abs_professional_flat') }} as flat

    left join {{ ref('ftr_professional_costs_categories') }} as costs
      on flat.claimId = costs.claimId
        and flat.lineId = costs.lineId

    left join {{ ref('ftr_professional_provider_categories') }} as provider_cat
      on flat.claimId = provider_cat.claimId
        and flat.lineId = provider_cat.lineId
    
    ## max(stayGroup) is intended as a safety measure in case a professional claim line is associated to more than one stayGroup. We do not believe this should happen, but in case it did, this piece would randomly pick the max of such stayGroups
    left join (select professionalClaimId, lineId,max(stayGroup) as stayGroup from   {{ ref('ftr_professional_admit_claims') }} group by 1,2) as prof_admits
      on flat.claimId = prof_admits.professionalClaimId
        and flat.lineId = prof_admits.lineId

    left join (select stayGroup, max(planOrUnplan) = 'Unplanned' as unplanned_admission from {{ref('ftr_planned_unplanned_admissions')}} group by 1) as unplanned
      on unplanned.stayGroup = prof_admits.stayGroup

    left join {{ ref('ftr_ip_surgical_medical') }} as surg_med
      on prof_admits.stayGroup = surg_med.stayGroup

    left join (select partner, max(dateFrom) as maxDateFrom from {{ ref('abs_professional_flat') }} group by partner) maxDate
        on maxDate.partner = flat.partner

    where flat.patientId is not null
      or (
          flat.partner = 'tufts' and 
          flat.patientId is null and 
          starts_with( flat.partnerMemberId , '0')
         ) -- tufts deidentified substance use claims
)

select * from professional_multimarket
