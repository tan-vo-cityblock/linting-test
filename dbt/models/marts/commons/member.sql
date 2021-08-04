with member as (

    select
        mbr.patientId,
        upper(concat(p.firstName," ", p.lastName)) as patientName,
        p.firstName,
        p.middleName,
        p.lastName,
        p.cityblockId,
        p.medicaidId,
        mrn.mrn,
        p.nmi,
        p.memberId,
        p.ssn,
        p.ssnEnd,
        p.doNotCall,
        p.enrollmentSource,
        mbr.cohortName,
        mbr.cohortGoLiveDate,
        ted.tuftsEnrollmentDate,
        mmc.patientHomeMarketName,
        mmc.patientHomeClinicName,
        mbr.partnerName,
        mbr.partnerId as planPartnerId,
        mcta.userId as primaryContactUserId,
        mcta.userName as primaryContactUserName,
        mcta.userEmail as primaryContactUserEmail,
        mcta.podName as memberPod,
        ms.currentState,
        date(ms.disenrolledAt) as disenrolledAtDate,
        pamt.isNoLongerInterested,
        pamt.isLostContact,
        pamt.hasDeclinedAssessments,
        pamt.hasDeclinedActionPlan,
        pamt.noLongerInterestedAt,
        pamt.lostContactAt,
        pamt.declinedAssessmentsAt,
        pamt.declinedActionPlanAt,
        p.lineOfBusiness,
        productDescription,
        dateOfBirth,
        mp.outreachPriority as isCurrentOutreachPriority,
        lp.patientId is not null as wasEverOutreachPriority,
        crl.fieldValue as covidRiskLevel,
        lrc.latestRatingCategory,
        case
          when mmc.patientHomeMarketName like '%Virtual%' then 'virtualIntegratedCare'
          when pdm.currentState is null then 'community'
          else pdm.currentState
        end as currentCareModel,
        pdm.currentState is null as isPresumedCareModel,
        la.pcpNpi as partnerAttributedPcpNpi,
        la.pcpProviderId as partnerAttributedPcpProviderId,
        la.pcpName as partnerAttributedPcpName,
        la.isCbhPcp as partnerAttributedIsCbhPcp,
        case when p.pcpPractice like '%ACP%' then p.pcpPractice
            else la.pcpOrganizationName end as partnerAttributedPcpOrganizationName,
        case when p.pcpPractice like '%ACP%' then true
            else la.isAcpnyPcp end as partnerAttributedIsAcpnyPcp,
        fpa.npi claimsAttributedPcpNpi,
        fpa.providerName claimsAttributedPcpName,
        fpa.cityblockProviderFlag claimsAttributedIsCbhPcp,
        fpa.providerAffiliatedGroupName claimsAttributedPcpOrganizationName,
        fpa.providerAffiliatedGroupName = 'Rapid Access Care @ ACP' as claimsAttributedIsAcpnyPcp,
        fpa.providerServicingId as claimsAttributedPcpServicingId,
        fpa.attributedPcp as claimsAttributedPcpCategory,
        emc.emblemMedicalCenterName,
        cast(su.fieldValue as boolean) as isSuperUtilizer,
        cpa.pathwaySlug as topPathwaySlug,
        cps.savingsDollarsPerMemberMonth as topPathwayProjectedSavingsPerMonth,
        mra.recommendedMemberAcuityScore,
        mra.recommendedMemberAcuityDescription,
        cast(cmm.fieldValue as boolean) as isComprehensiveMedicationManagementCandidate,
        cast(crf.fieldValue as int64) as comprehensiveMedicationManagementRiskFactorCount,
        dma.deliveryModel as suggestedDeliveryModel

    from {{ ref('src_member') }} as mbr

    left join {{ source('commons', 'patient') }} as p
        on mbr.patientId = p.id

    inner join {{ ref('abs_commons_member_market_clinic') }} as mmc
        on p.id = mmc.patientId

    left join {{ ref('primary_care_assignment') }}  as la
        on p.id=la.patientId and la.isLatestAssignment = true

    left join {{ ref('member_current_care_team_all') }} as mcta
        on p.id = mcta.patientId and mcta.isPrimaryContact

    left join {{ ref('member_states') }} as ms
        on p.id = ms.patientId

    left join {{ ref('abs_member_programs') }} as mp
        on p.id = mp.patientId

    left join {{ ref('abs_legacy_prioritization') }} lp
        on p.id = lp.patientId

    left join {{ ref('tufts_enrollment_date') }} as ted
        on p.id = ted.patientId

    left join {{ ref('cf_covid_risk_level') }} as crl
        on p.id = crl.patientId

    left join {{ ref('latest_rating_category') }} as lrc
        on mbr.patientId = lrc.patientId

    left join {{ source('commons', 'patient_delivery_model') }} as pdm
        on p.id = pdm.patientId and pdm.deletedAt is null

    left join {{ ref('ftr_pcp_attribution') }} as fpa
        on mbr.patientId = fpa.patientId

    left join {{ ref('abs_emblem_member_medical_center') }} as emc
        on mbr.patientId = emc.patientId

    left join {{ ref('patient_attribute_member_types') }} as pamt
        on mbr.patientId = pamt.patientId

    left join {{ source('member_index', 'mrn') }} as mrn
        on mbr.mrnId = mrn.id

    left join {{ ref('cf_super_utilizer') }} as su
        on mbr.patientId = su.patientId

    left join {{ ref('mrt_care_pathway_assignment') }} as cpa
        on mbr.patientId = cpa.patientId and cpa.memberPathwayRank = 1

    left join {{ ref('mrt_care_pathway_savings') }} as cps
        on mbr.patientId = cps.patientId

    left join {{ ref('member_recommended_acuity') }} as mra
        on  mbr.patientId = mra.patientId

    left join {{ ref('cf_comprehensive_medication_management') }} as cmm
        on mbr.patientId = cmm.patientId

    left join {{ ref('cf_cmm_risk_factors') }} as crf
        on mbr.patientId = crf.patientId

    left join {{ ref('mrt_delivery_model_assignments') }} as dma
        on mbr.patientId = dma.patientId

)

select * from member
