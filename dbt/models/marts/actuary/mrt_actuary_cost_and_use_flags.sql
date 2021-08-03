{{
 config(
   materialized='view'
 )
}}

select
    -- joinable fields
    partner,
    formType,
    patientId,
    partnerMemberId,
    partnerClaimId,
    lineNumber,
    -- cbh specific joinable fields; maybe less useful for actuary
    commonId,
    surrogateId,
    claimId,
    -- c&u flags/categories
    claimLineStatus,
    ifnull(costCategory, 'other') as costCategory,
    costSubCategory,
    costSubCategoryDetail,
    costCategoryLine, -- Only applies to professional claims
    costSubCategoryLine, -- Only applies to professional claims
    costSubCategoryDetailLine, -- Only applies to professional claims
    capitatedParty,
    capitatedFlag,
    -- lesli's 5 & 12 buckets here
    costBucketMajor,
    costBucketMinor,
    acuteInpatientCategoryFlag,
    readmissionFlag,
    resultsInReadmissionFlag,
    icuServiceFlag,
    edCategoryFlag,
    edServiceFlag,
    obsCategoryFlag,
    providerCategory,
    primaryCareProviderFlag,
    surgicalProviderFlag,
    specialistProviderFlag,
    behavioralHealthProviderFlag,
    -- ambulatory care sensitive visits, based in diagnosis code
    acsInpatientFlag,
    acsInpatientTypes,
    acsEdFlag

from {{ ref('mrt_claims_self_service_all') }}
