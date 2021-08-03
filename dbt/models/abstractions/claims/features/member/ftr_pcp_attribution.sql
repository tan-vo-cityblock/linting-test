WITH
    -- Reference tables
    cbh_provider_roster as (
        SELECT npi AS cbhRosterNPI
        FROM {{ ref('src_cityblock_providers') }}
    ),
    pcp_attribution_codes as (
        SELECT code, codeType
        FROM {{ ref('pcp_attribution_codes') }}
    ),
    -- START: Provider Data
    providers_step_0 AS (
        -- Grab provider data and prepare for dedup
        SELECT 
              npi AS providerNPI
            , partner AS providerPartner
            , partnerProviderId
            , providerId
            , ROW_NUMBER() OVER(
                PARTITION BY providerId 
                ORDER BY dateEffectiveTo IS NULL DESC, 
                            dateEffectiveTo DESC, 
                            affiliatedGroupName IS NOT NULL DESC) 
              AS dedupProviderId
            , name AS providerName
            , affiliatedGroupName AS providerAffiliatedGroupName
            , taxonomiesCode AS providerTaxonomyCode
            , taxonomiesTier AS providerTaxonomyTier
        FROM {{ ref('abs_provider_flat') }}
    ),
    providers AS (
        -- Ready provider claims data with roster data
        -- Includes *all* markets (e.g., CT, DC, MA, NY, etc)
        SELECT
            CASE WHEN cbhRosterNPI IS NOT NULL THEN 1 ELSE 0 END isCBHProvider
            , providerNPI
            , providerPartner
            , partnerProviderId
            , providerId
            , providerName
            , providerAffiliatedGroupName
            , providerTaxonomyCode
            , providerTaxonomyTier
        FROM providers_step_0 AS p
        LEFT JOIN cbh_provider_roster r
            ON p.providerNPI = r.cbhRosterNPI
        WHERE dedupProviderId = 1
    ),
    -- START: Claims data
    professional_claims AS (
        -- Ready professional data
        SELECT DISTINCT
            claimId
            , partner
            , patientId
            , providerBillingId AS providerBillingId
            , providerServicingId
            , providerServicingSpecialty
            , placeOfService
            , dateFrom
            , procedureCode
            , prov.codeType as providerServicingType
        FROM {{ ref('abs_professional_flat') }} p
        INNER JOIN pcp_attribution_codes em 
            ON p.procedureCode = em.code
            AND em.codeType = 'evaluationAndManagement'
        INNER JOIN pcp_attribution_codes pos 
            ON p.placeOfService = pos.code
            AND pos.codeType = 'placeOfService'
        INNER JOIN pcp_attribution_codes prov
            ON p.providerServicingSpecialty = prov.code
            AND prov.codeType in ('primaryCareProvider', 'specialtyCareProvider')
        WHERE patientId IS NOT NULL
    ),
    -- START: Member data
    member_states AS (
        -- Grab member state information
        SELECT
            patientId
            , currentState
            , CAST(consentedAt AS DATE) as consentedAtDate
        FROM {{ ref('member_states') }}
    ),
    members AS (
        -- Ready member data with state 
        SELECT
            m.patientId
            , s.currentState
            , s.consentedAtDate
        FROM {{ ref('src_member') }} AS m
        LEFT JOIN member_states AS s USING(patientId)
    ),
    -- START: Create features for algorithm logic
    features_step_0 AS (
        -- Join all provider, professional, and member data together -- include only members with professional claims. Change order of join if need all members.
        SELECT
            m.*
            , prof.* except(patientId)
            , prov.*
            , CURRENT_DATE() AS currentDate
            , CASE 
                WHEN currentState = 'consented' or currentState = 'enrolled'
                THEN DATE_DIFF(CURRENT_DATE(), consentedAtDate, MONTH)
                ELSE NULL 
            END as consentedMonths
            , DATE_DIFF(CURRENT_DATE(), consentedAtDate, MONTH) >= 6 
                AND (currentState = 'consented' or currentState = 'enrolled') as isConsentedMoreThanSixMonths
        FROM professional_claims AS prof
        -- Filters out out-of-network providers (not in provider roster)
        INNER JOIN providers AS prov
            ON prof.providerServicingId = prov.providerId
        INNER JOIN members AS m
            ON prof.patientId = m.patientId
    ),
    -- Specify the lookback period based on the type of member
    features_step_1 AS (
        SELECT
            *
            , CASE
            -- If consented >=6 months, lookback period = # consented months
                WHEN isConsentedMoreThanSixMonths
                THEN consentedMonths
                ELSE 12
            END AS lookBackMonths
            , CASE
                WHEN isConsentedMoreThanSixMonths
                THEN DATE_SUB(currentDate, INTERVAL consentedMonths MONTH)
                ELSE DATE_SUB(currentDate, INTERVAL 12 MONTH)
            END AS lookBackDate
        FROM features_step_0
    ),
    features_step_2 AS (
        -- For each patient/provider combination determine:
        -- (1) the number of visits (with prep for CBH minus 2 adjustment) and
        -- (2) the most recent visit during the lookback period
        SELECT
            *
            , SUM(1)
                OVER (
                    PARTITION BY patientId, providerNPI
                ) AS unadjusted_num_visits
            , MAX(dateFrom)
                OVER (
                    PARTITION BY patientId, providerNPI
                ) AS last_known_visit
        FROM features_step_1
        -- Remove visits before lookback period, else will attribute PCP for members without visit in lookback
        WHERE dateFrom >= lookBackDate
    ),
    features_step_3 AS (
        -- For each patient/provider combination determine:
        -- (1) the number of visits (with CBH minus 2 adjustment) and
        -- (2) whether primary care or not
        SELECT
            *
            , CASE
                WHEN isCBHProvider = 1
                THEN unadjusted_num_visits - 2
                ELSE unadjusted_num_visits
            END as cbh_adjusted_num_visits
            , CASE
                WHEN providerServicingType = 'primaryCareProvider'
                THEN 1
                WHEN providerServicingType = 'specialtyCareProvider'
                THEN 0
                ELSE NULL
            END AS isPrimaryCareProvider,
            -- Rank for pulling in provider fields in final table
            ROW_NUMBER() OVER(PARTITION BY patientId, providerNPI order by dateFrom DESC) as rowNumVisit
        FROM features_step_2
    ),
    features_step_4 AS (
        SELECT * except (rowNumVisit)
        FROM features_step_3
        WHERE rowNumVisit = 1
    ),
    ---- START: algorithm logic
    algorithm_step_0 AS (
        -- Per patient ranking logic:
        -- 1. Take primary care over specialists
        -- 2. Order by CBH-adjusted number of visits first, then use unadjusted number to break ties, and last known visit for further ties
        SELECT DISTINCT
            *      
            , ROW_NUMBER()
                OVER (
                    PARTITION BY patientId
                    ORDER BY isPrimaryCareProvider DESC, 
                        cbh_adjusted_num_visits DESC, 
                        unadjusted_num_visits DESC, 
                        last_known_visit DESC
            ) AS providerRank
        FROM features_step_4
    ),
    algorithm_step_1 AS (
        -- Grab the highest ranking provider based-on our ranking logic
        SELECT *
        FROM algorithm_step_0
        WHERE providerRank = 1
    ),
    -- Roll back up to member level attributes
    algorithm_results as (
        SELECT
            patientId,
            partner,
            partnerProviderId,
            providerNPI as npi,
            providerId,
            providerServicingId, -- Note: eventually may need this as an array of all IDs to merge back to claims
            providerBillingId,
            providerName,
            providerTaxonomyCode,
            providerTaxonomyTier,
            providerAffiliatedGroupName,
            CASE 
                WHEN isCBHProvider = 1 
                THEN True 
                ELSE False 
            END as cityblockProviderFlag,
            CASE
                WHEN isPrimaryCareProvider = 1
                THEN 'primary'
                ELSE 'specialty'
            END as stringPrimarySpecialtyIndicator,
            last_known_visit as lastKnownVisit,
            unadjusted_num_visits as numVisitsInLookback,
            lookBackDate as lookbackDate,
            lookBackMonths as lookbackDurationMonths,
            CASE
                WHEN isCBHProvider = 1 AND isPrimaryCareProvider = 1
                THEN 'cbh primary'
                WHEN isCBHProvider = 1 AND isPrimaryCareProvider = 0
                THEN 'cbh specialist'
                WHEN isCBHProvider = 0 AND isPrimaryCareProvider = 1
                THEN 'non-cbh primary'
                WHEN isCBHProvider = 0 AND isPrimaryCareProvider = 0
                THEN 'non-cbh specialist'
                ELSE NULL
            END as attributedPcp
        FROM algorithm_step_1       
    )

SELECT * FROM algorithm_results
