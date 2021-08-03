
with facility_multimarket as (

    select
        -- set of base columns from the flat table
        'medical' as claimType,
        'facility' as formType,
        flat.partner,
        flat.partnerSourceName,

        flat.claimId,
        flat.partnerClaimId,
        flat.lineNumber,
        flat.lineId,

        flat.procedureCode,
        flat.revenueCode,

        flat.serviceQuantity,

        flat.partnerMemberId,
        flat.commonId,
        flat.patientId,

        flat.lineOfBusiness,
        flat.subLineOfBusiness,

        flat.providerServicingId,
        flat.providerServicingSpecialty,

        flat.providerBillingId,

        flat.typeOfBill as billType,
        STRING(NULL) as placeOfService, -- Only applies to professional claims

        flat.dateFrom,
        flat.dateTo,

        flat.datePaid,

        flat.dateAdmit,
        flat.dateDischarge,
        flat.dischargeStatus,

        flat.amountAllowed,
        flat.amountPlanPaid,

        flat.principalProcedureCode,
        flat.principalDiagnosisCode,
        flat.claimLineStatus,

        flat.surrogateId,

        STRING(NULL) as ndc, -- Only applies to pharmacy claims
        NULL as daysSupply, -- Only applies to pharmacy claims
        STRING(NULL) as brandIndicator, -- Only applies to pharmacy claims
        string(null) as npi, -- Only applies to pharmacy claims

        -- columns added on via joins to feature tables

        locations.outpatientLocationCategory as outpatientLocation,

        costs.costCategory as claimCategory,
        costs.costSubCategory as serviceCategory,
        costs.costSubCategoryDetail as serviceSubCategory,
        costs.costCategory,
        costs.costSubCategory,
        costs.costSubCategoryDetail,
        STRING(NULL) as costCategoryLine, -- Only applies to professional claims
        STRING(NULL) as costSubCategoryLine, -- Only applies to professional claims
        STRING(NULL) as costSubCategoryDetailLine, -- Only applies to professional claims

        case when costs.costCategory = 'inpatient' and costs.costSubCategory = 'acute' then TRUE else FALSE end as acuteInpatientCategoryFlag,
        case when costs.costCategory = 'outpatient' and costs.costSubCategory = 'ed' then TRUE else FALSE end edCategoryFlag,
        case when costs.costCategory = 'outpatient' and costs.costSubCategory = 'obs' then TRUE else FALSE end obsCategoryFlag,
        

        readmits.readmissionFlag,
        readmits.resultsInReadmissionFlag,

        services.edServiceFlag,
        services.icuServiceFlag,

        stays.stayType,
        stays.stayGroup,
        concat(flat.patientId,'-',cast(flat.dateFrom as string) ) as patientDate,
        cast(surg_med.surgical as string) as surgical,
        flat.drgCode,
        flat.drgCodeset,
        cast(unplanned.unplanned_admission as string) as unplanned_admission,

        STRING(NULL) as providerCategory, -- Only applies to professional claims
        FALSE as primaryCareProviderFlag, -- Only applies to professional claims
        FALSE as surgicalProviderFlag, -- Only applies to professional claims
        FALSE as specialistProviderFlag, -- Only applies to professional claims
        FALSE as behavioralHealthProviderFlag, -- Only applies to professional claims

        inpatient_acs.acsInpatientFlag,
        inpatient_acs.acsInpatientTypes,

        ed_acs.acsEdFlag,

        maxDate.maxDateFrom

    from {{ ref('abs_facility_flat') }} as flat

    left join {{ ref('ftr_facility_locations_categories') }} as locations
      on flat.claimId = locations.claimId



    left join {{ ref('ftr_facility_costs_categories') }} as costs
      on flat.claimId = costs.claimId

    left join {{ ref('ftr_inpatient_readmissions') }} as readmits
      on flat.claimId = readmits.claimId

    left join {{ ref('ftr_facility_services_flags') }} as services
      on flat.claimId = services.claimId

    left join {{ ref('ftr_facility_stays') }} as stays
      on flat.claimId = stays.claimId

    left join (select stayGroup, max(planOrUnplan) ='Unplanned' as unplanned_admission from {{ref('ftr_planned_unplanned_admissions')}} group by 1) as unplanned
      on stays.stayGroup = unplanned.stayGroup

    left join {{ ref('ftr_ip_surgical_medical') }} as surg_med
      on stays.stayGroup = surg_med.stayGroup

    left join {{ ref('ftr_inpatient_indicators_acs_categories') }} as inpatient_acs
      on flat.claimId = inpatient_acs.claimId

    left join {{ ref('ftr_ed_indicators_acs_values') }} as ed_acs
      on flat.claimId = ed_acs.claimId

    left join (select partner, max(dateFrom) as maxDateFrom from {{ ref('abs_facility_flat') }} group by partner) as maxDate
      on maxDate.partner = flat.partner

    where flat.patientId is not null
      or (
          flat.partner = 'tufts' and 
          flat.patientId is null and 
          starts_with( flat.partnerMemberId , '0')
         ) -- tufts deidentified substance use claims
)

select * from facility_multimarket
