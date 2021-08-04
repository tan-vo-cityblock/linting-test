with info as (
  select patientId, 
  patientName
  from `cityblock-analytics.mrt_commons.member` ),
  
patient_documents as (
    select patientId,
           earliestPrivacyPracticesNoticeAt,
           latestPrivacyPracticesNoticeAt,
           earliestPlanOfCareAttestationAt,
           latestPlanOfCareAttestationAt,
           earliestTreatmentConsentAt,
           latestTreatmentConsentAt,
           earliestPhiSharingConsentAt,
           latestPhiSharingConsentAt
    from `cityblock-analytics.abs_commons.patient_document_types`
),

docs as (
  select patientId,
  hasHealthcareProxy,
  hasMolst, 
  hasAdvanceDirectives,
  minAcpDocumentUploadAt
  from `cityblock-analytics.mrt_commons.member_commons_completion` ),

final as (
  select i.patientId as memberId,
  i.patientName as memberName,
  d.hasHealthcareProxy,
  d.hasMolst, 
  d.hasAdvanceDirectives,
  d.minAcpDocumentUploadAt,
  pd. earliestPrivacyPracticesNoticeAt,
  pd.latestPrivacyPracticesNoticeAt,
  pd.earliestPlanOfCareAttestationAt,
  pd.latestPlanOfCareAttestationAt,
  pd.earliestTreatmentConsentAt,
  pd.latestTreatmentConsentAt,
  pd.earliestPhiSharingConsentAt,
  pd.latestPhiSharingConsentAt
  from info i
  inner join docs d
  on i.patientId = d.patientId
  inner join patient_documents pd
  on pd.patientId = i.patientId
 )

select * from final
