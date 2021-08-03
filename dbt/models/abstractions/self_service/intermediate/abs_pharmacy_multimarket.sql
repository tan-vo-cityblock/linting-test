
with pharmacy_multimarket as (

    select
        -- set of base columns from the flat table
        'pharmacy' as claimType,
        'pharmacy' as formType,
        flat.partner,
        flat.partnerSourceName,

        flat.claimId,
        flat.partnerClaimId,
        (case when flat.partner = 'tufts' then flat.fillNumber else 1 end) as lineNumber,
        STRING(Null) as lineId, -- We have not built this concept on pharmacy claims

        STRING(NULL) as procedureCode,
        STRING(NULL) as revenueCode,

        flat.quantityDispensed as serviceQuantity,

        flat.partnerMemberId,
        flat.commonId,
        flat.patientId,

        flat.lineOfBusiness,
        flat.subLineOfBusiness,

        -- for the servicing provider map to the prescribr
        flat.prescriberId as providerServicingId,
        flat.prescriberSpecialty as providerServicingSpecialty,

        -- for the billing provider map to the pharmacy
        flat.pharmacyId as providerBillingId,

        STRING(NULL) as billType, -- Only applies to facility claims
        STRING(NULL) as placeOfService, -- Only applies to professional claims

        -- use fill date as the from and to
        flat.dateFilled as dateFrom,
        flat.dateFilled as dateTo,

        flat.datePaid,

        cast(NULL as date) as dateAdmit, -- Only applies to facility claims
        cast(NULL as date) as dateDischarge, -- Only applies to facility claims
        STRING(NULL) as dischargeStatus, -- Only applies to facility claims

        flat.amountAllowed,
        flat.amountPlanPaid,

        STRING(NULL) as principalProcedureCode, -- Only applies to facility / prof claims
        STRING(NULL) as principalDiagnosisCode, -- Only applies to facility / prof claims
        flat.claimLineStatus,

        flat.surrogateId,

        flat.ndc,
        flat.daysSupply,
        flat.brandIndicator,
        flat.pharmacyNpi as npi,

        -- columns added on via joins to feature tables

        STRING(NULL) as outpatientLocation, -- Only applies to facility claims

        costs.costCategory as claimCategory,
        costs.costSubCategory as serviceCategory,
        costs.costSubCategoryDetail as serviceSubCategory,
        costs.costCategory,
        costs.costSubCategory,
        costs.costSubCategoryDetail,
        STRING(NULL) as costCategoryLine, -- Only applies to professional claims
        STRING(NULL) as costSubCategoryLine, -- Only applies to professional claims
        STRING(NULL) as costSubCategoryDetailLine, -- Only applies to professional claims

        FALSE as acuteInpatientCategoryFlag, -- Only applies to facility claims
        FALSE as edCategoryFlag, -- Only applies to facility claims
        FALSE as obsCategoryFlag, -- Only applies to facility claims

        FALSE as readmissionFlag, -- Only applies to facility claims
        FALSE as resultsInReadmissionFlag, -- Only applies to facility claims

        FALSE as edServiceFlag, -- Only applies to facility claims
        FALSE as icuServiceFlag, -- Only applies to facility claims

        STRING(NULL) as stayType, -- Only applies to facility claims
        STRING(NULL) as stayGroup, -- Only applies to facility claims
        concat(flat.patientId,'-',cast(flat.dateFilled as string) ) as patientDate,
        STRING(NULL) as surgical,
        STRING(NULL) as drgCode,
        STRING(NULL) as drgCodeset,
        STRING(NULL) as unplanned_admission,
        STRING(NULL) as providerCategory, -- Only applies to professional claims
        FALSE as primaryCareProviderFlag, -- Only applies to professional claims
        FALSE as surgicalProviderFlag, -- Only applies to professional claims
        FALSE as specialistProviderFlag, -- Only applies to professional claims
        FALSE as behavioralHealthProviderFlag, -- Only applies to professional claims

        FALSE as acsInpatientFlag, -- Only applies to inpatient facility claims
        STRING(NULL) as acsInpatientTypes, -- Only applies to inpatient facility claims

        FALSE as acsEdFlag, -- Only applies to ed facility claims

        maxDateFrom

    from {{ ref('abs_pharmacy_flat') }} as flat

    left join {{ ref('ftr_pharmacy_costs_categories') }} as costs
      on flat.claimId = costs.claimId
      and flat.fillNumber = costs.fillNumber

    -- use the fill date to determine the most recent data
    left join (select partner, max(dateFilled) as maxDateFrom from {{ ref('abs_pharmacy_flat') }} group by partner) maxDate
        on maxDate.partner = flat.partner

    where flat.patientId is not null
      or (
          flat.partner = 'tufts' and 
          flat.patientId is null and 
          starts_with( flat.partnerMemberId , '0')
         ) -- tufts deidentified substance use claims
)

select * from pharmacy_multimarket
