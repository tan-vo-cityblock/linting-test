
with medical_union as (

	select * from {{ ref('abs_facility_multimarket') }}

	union all

	select * from {{ ref('abs_professional_multimarket') }}

	union all

	select * from {{ ref('abs_pharmacy_multimarket') }}

),

final as (

  select
    medical_union.*,
    edServiceFlag and acuteInpatientCategoryFlag as resultsInInpatientFlag,
    cap.* except(claimId, lineId),

  -- lesli's 5 & 12 buckets here
  case
    when 	costCategory = 'inpatient'	 and costSubCategory = 'maternity' then 'Inpatient'
    when 	costCategory = 'inpatient'	 and costSubCategory = 'rehab' then 'Inpatient'
    when 	costCategory = 'inpatient'	 and costSubCategory = 'acute' and costSubCategoryDetail = 'acute' then 'Inpatient'
    when 	costCategory = 'inpatient'	 and costSubCategory = 'maternity' and costSubCategoryDetail = 'childbirth'	then 'Inpatient'
    when 	costCategory = 'inpatient'	 and costSubCategory = 'acute' and costSubCategoryDetail = 'icu' then 'Inpatient'
    when 	costCategory = 'inpatient'	 and costSubCategory = 'maternity' and costSubCategoryDetail = 'newborn' then 'Inpatient'
    when 	costCategory = 'inpatient'	 and costSubCategory = 'hospice' and costSubCategoryDetail = 'other' then 'Inpatient'
    when 	costCategory = 'inpatient'	 and costSubCategory = 'other' and costSubCategoryDetail = 'other' then 'Inpatient'
    when 	costCategory = 'inpatient'	 and costSubCategory = 'psych' and costSubCategoryDetail = 'psych' then 'Inpatient'
    when 	costCategory = 'inpatient'	 and costSubCategory = 'psych' and costSubCategoryDetail = 'substanceAbuse' then 'Inpatient'
    when 	costCategory = 'inpatient'	 and costSubCategory = 'rehab' and costSubCategoryDetail = 'substanceAbuse' then 'Inpatient'
    when 	costCategory = 'outpatient'	 and (costSubCategory is null or costSubCategory = 'other') then 'Outpatient'
    when 	costCategory = 'outpatient'	 and costSubCategory = 'ambulance' then 'Outpatient'
    when 	costCategory = 'outpatient'	 and costSubCategory = 'behavioralHealth' then 'Outpatient'
    when 	costCategory = 'outpatient'	 and costSubCategory = 	'dialysis' then 'Outpatient'
    when 	costCategory = 'outpatient'	 and costSubCategory = 'dme' then 'Outpatient'
    when 	costCategory = 'outpatient'	 and costSubCategory = 'ed' then 'Outpatient'
    when 	costCategory = 'outpatient'	 and costSubCategory = 'homeHealth' then 'Outpatient'
    when 	costCategory = 'outpatient'	 and costSubCategory = 'hospice' then 'Outpatient'
    when 	costCategory = 'outpatient'	 and costSubCategory = 'infusion' then 'Outpatient'
    when 	costCategory = 'outpatient'	 and costSubCategory = 'labs' then 'Outpatient'
    when 	costCategory = 'outpatient'	 and costSubCategory = 'obs' then 'Outpatient'
    when 	costCategory = 'outpatient'	 and costSubCategory = 'other' then 'Outpatient'
    when 	costCategory = 'outpatient'	 and costSubCategory = 'radiology' then 'Outpatient'
    when 	costCategory = 'outpatient'	 and costSubCategory = 'surgery' then 'Outpatient'
    when 	costCategory = 'pharmacy'	 and costSubCategory = 'pharmacy' then 'RX'
    when 	costCategory = 'professional'	 and costSubCategory = 'ambulance' and costSubCategoryDetail = 'professional' then 'Outpatient'
    when 	costCategory = 'professional'	 and costSubCategory = 'behavioralHealth' and costSubCategoryDetail = 'professional' then 'Outpatient'
    when 	costCategory = 'professional'	 and costSubCategory = 'dialysis' and costSubCategoryDetail = 'professional' then 'Outpatient'
    when 	costCategory = 'professional'	 and costSubCategory = 'dme' and costSubCategoryDetail = 'professional' then 'Outpatient'
    when 	costCategory = 'professional'	 and costSubCategory = 'ed' and costSubCategoryDetail = 'professional' then 'Outpatient'
    when 	costCategory = 'professional'	 and costSubCategory = 'homeHealth' and costSubCategoryDetail = 'professional' then 'Outpatient'
    when 	costCategory = 'professional'	 and costSubCategory = 'hospice'  then 'Outpatient'
    when 	costCategory = 'professional'	 and costSubCategory = 'infusion' and costSubCategoryDetail = 'professional' then 'Outpatient'
    when 	costCategory = 'professional'	 and costSubCategory = 'inpatient' and costSubCategoryDetail = 'professional' then 'Inpatient'
    when 	costCategory = 'professional'	 and costSubCategory = 'labs' and costSubCategoryDetail = 'professional' then 'Outpatient'
    when 	costCategory = 'professional'	 and costSubCategory = 'inpatientMaternity' and costSubCategoryDetail = 'professional' then 'Inpatient'
    when 	costCategory = 'professional'	 and costSubCategory = 'obs' and costSubCategoryDetail = 'professional' then 'Outpatient'
    when 	costCategory = 'professional'	 and costSubCategory = 'other' and costSubCategoryDetail = 'professional' then 'Outpatient'
    when 	costCategory = 'professional'	 and costSubCategory = 'primaryCare' and costSubCategoryDetail = 'professional' then 'Prof Office'
    when 	costCategory = 'professional'	 and costSubCategory = 'inpatientPsych' and costSubCategoryDetail = 'professional' then 'Inpatient'
    when 	costCategory = 'professional'	 and costSubCategory = 'radiology' and costSubCategoryDetail = 'professional' then 'Outpatient'
    when 	costCategory = 'professional'	 and costSubCategory = 'rehab' and costSubCategoryDetail = 'professional' then 'Inpatient'
    when 	costCategory = 'professional'	 and costSubCategory = 'snf' and costSubCategoryDetail = 'professional' then 'SNF'
    when 	costCategory = 'professional'	 and costSubCategory = 'specialtyCare' and costSubCategoryDetail = 'professional' then 'Prof Office'
    when 	costCategory = 'professional'	 and costSubCategory = 'surgery' and costSubCategoryDetail = 'professional' then 'Outpatient'
    when 	costCategory = 'snf'	 and costSubCategory = 'acute' then 'SNF'
    when 	costCategory = 'snf'	 and costSubCategory = 'subAcute' then 'SNF'
    else null end as costBucketMajor,

  case
    when	costCategory = 'inpatient'	and costSubCategory = 'maternity' then 'Acute Inpatient'
    when	costCategory = 'inpatient'	and costSubCategory = 'rehab' then 'Sub-Acute Care'
    when	costCategory = 'inpatient'	and costSubCategory = 'acute' and costSubCategoryDetail = 'acute' then 'Acute Inpatient'
    when	costCategory = 'inpatient'	and costSubCategory = 'maternity' and costSubCategoryDetail = 'childbirth'	then 'Acute Inpatient'
    when	costCategory = 'inpatient'	and costSubCategory = 'acute' and costSubCategoryDetail = 'icu' then 'Acute Inpatient'
    when	costCategory = 'inpatient'	and costSubCategory = 'maternity' and costSubCategoryDetail = 'newborn' then 'Acute Inpatient'
    when	costCategory = 'inpatient'	and costSubCategory = 'hospice' and costSubCategoryDetail = 'other' then 'Acute Inpatient'
    when	costCategory = 'inpatient'	and costSubCategory = 'other' and costSubCategoryDetail = 'other' then 'Acute Inpatient'
    when	costCategory = 'inpatient'	and costSubCategory = 'psych' and costSubCategoryDetail = 'psych' then 'Inpatient Behavioral Health'
    when	costCategory = 'inpatient'	and costSubCategory = 'psych' and costSubCategoryDetail = 'substanceAbuse' then 'Inpatient Behavioral Health'
    when	costCategory = 'inpatient'	and costSubCategory = 'rehab' and costSubCategoryDetail = 'substanceAbuse' then 'Inpatient Behavioral Health'
    when	costCategory = 'outpatient'	and (costSubCategory is null or costSubCategory = 'other') then 'Other Outpatient'
    when	costCategory = 'outpatient'	and costSubCategory = 'ambulance' then 'Emergency Services'
    when	costCategory = 'outpatient'	and costSubCategory = 'behavioralHealth' then 'Outpatient Behavioral Health'
    when	costCategory = 'outpatient'	and costSubCategory =	'dialysis' then 'Dialysis'
    when	costCategory = 'outpatient'	and costSubCategory = 'dme' then 'Other Outpatient'
    when	costCategory = 'outpatient'	and costSubCategory = 'ed' then 'Emergency Services'
    when	costCategory = 'outpatient'	and costSubCategory = 'homeHealth' then 'Home Health'
    when	costCategory = 'outpatient'	and costSubCategory = 'hospice' then 'Sub-Acute Care'
    when	costCategory = 'outpatient'	and costSubCategory = 'infusion' then 'Other Outpatient'
    when	costCategory = 'outpatient'	and costSubCategory = 'labs' then 'Other Outpatient'
    when	costCategory = 'outpatient'	and costSubCategory = 'obs' then 'Emergency Services'
    when	costCategory = 'outpatient'	and costSubCategory = 'other' then 'Other Outpatient'
    when	costCategory = 'outpatient'	and costSubCategory = 'radiology' then 'Specialty Care'
    when	costCategory = 'outpatient'	and costSubCategory = 'surgery' then 'Outpatient Procedures'
    when	costCategory = 'pharmacy'	and costSubCategory = 'pharmacy' then 'RX'
    when	costCategory = 'professional'	and costSubCategory = 'ambulance' and costSubCategoryDetail = 'professional' then 'Emergency Services'
    when	costCategory = 'professional'	and costSubCategory = 'behavioralHealth' and costSubCategoryDetail = 'professional' then 'Outpatient Behavioral Health'
    when	costCategory = 'professional'	and costSubCategory = 'dialysis' and costSubCategoryDetail = 'professional' then 'Dialysis'
    when	costCategory = 'professional'	and costSubCategory = 'dme' and costSubCategoryDetail = 'professional' then 'Other Outpatient'
    when	costCategory = 'professional'	and costSubCategory = 'ed' and costSubCategoryDetail = 'professional' then 'Emergency Services'
    when	costCategory = 'professional'	and costSubCategory = 'homeHealth' and costSubCategoryDetail = 'professional' then 'Home Health'
    when	costCategory = 'professional'	and costSubCategory = 'hospice'  then 'Sub-Acute Care'
    when	costCategory = 'professional'	and costSubCategory = 'infusion' and costSubCategoryDetail = 'professional' then 'Other Outpatient'
    when	costCategory = 'professional'	and costSubCategory = 'inpatient' and costSubCategoryDetail = 'professional' then 'Acute Inpatient'
    when	costCategory = 'professional'	and costSubCategory = 'labs' and costSubCategoryDetail = 'professional' then 'Other Outpatient'
    when	costCategory = 'professional'	and costSubCategory = 'inpatientMaternity' and costSubCategoryDetail = 'professional' then 'Acute Inpatient'
    when	costCategory = 'professional'	and costSubCategory = 'obs' and costSubCategoryDetail = 'professional' then 'Other Outpatient'
    when	costCategory = 'professional'	and costSubCategory = 'other' and costSubCategoryDetail = 'professional' then 'Other Outpatient'
    when	costCategory = 'professional'	and costSubCategory = 'primaryCare' and costSubCategoryDetail = 'professional' then 'Primary Care'
    when	costCategory = 'professional'	and costSubCategory = 'inpatientPsych' and costSubCategoryDetail = 'professional' then 'Inpatient Behavioral Health'
    when	costCategory = 'professional'	and costSubCategory = 'radiology' and costSubCategoryDetail = 'professional' then 'Other Outpatient'
    when	costCategory = 'professional'	and costSubCategory = 'rehab' and costSubCategoryDetail = 'professional' then 'Sub-Acute Care'
    when	costCategory = 'professional'	and costSubCategory = 'snf' and costSubCategoryDetail = 'professional' then 'Sub-Acute Care'
    when	costCategory = 'professional'	and costSubCategory = 'specialtyCare' and costSubCategoryDetail = 'professional' then 'Specialty Care'
    when	costCategory = 'professional'	and costSubCategory = 'surgery' and costSubCategoryDetail = 'professional' then 'Outpatient Procedures'
    when	costCategory = 'snf'	and costSubCategory = 'acute' then 'Sub-Acute Care'
    when	costCategory = 'snf'	and costSubCategory = 'subAcute' then 'Sub-Acute Care'
    else null end as costBucketMinor,






     case
    when    costCategory = 'inpatient'   and costSubCategory = 'maternity' then 'IP Maternity'
    when    costCategory = 'inpatient'   and costSubCategory = 'rehab' and costSubCategoryDetail ='substanceAbuse' then 'IP Subs Use'
    when    costCategory = 'inpatient'   and costSubCategory = 'rehab' and costSubCategoryDetail !='substanceAbuse' then 'IP Rehab'
    when    costCategory = 'inpatient'   and costSubCategory = 'rehab' and costSubCategoryDetail is null then 'IP Rehab'
    when    costCategory = 'inpatient'   and costSubCategory = 'psych' and costSubCategoryDetail ='substanceAbuse' then 'IP Subs Use'
    when    costCategory = 'inpatient'   and costSubCategory = 'psych' and costSubCategoryDetail !='substanceAbuse' then 'IP Psych'
    when    costCategory = 'inpatient'   and costSubCategory = 'psych' and costSubCategoryDetail is null then 'IP Psych'
    when    costCategory = 'inpatient'   and costSubCategory in ('acute','other') then 'Acute IP'
    when    costCategory = 'inpatient'  and costSubCategory = 'hospice'  then 'Hospice'
    when    costCategory = 'outpatient'  and (costSubCategory is null or costSubCategory = 'other') then 'OP Other'
    when    costCategory = 'outpatient'  and costSubCategory = 'ambulance' then 'Transport'
    when    costCategory = 'outpatient'  and costSubCategory = 'behavioralHealth' then 'OP Psych'
    when    costCategory = 'outpatient'  and costSubCategory = 'dialysis' then 'Dialysis'
    when    costCategory = 'outpatient'  and costSubCategory = 'dme' then 'DME'
    when    costCategory = 'outpatient'  and costSubCategory = 'ed' then 'ED'
    when    costCategory = 'outpatient'  and costSubCategory = 'homeHealth' then 'Home Health'
    when    costCategory = 'outpatient'  and costSubCategory = 'hospice' then 'Hospice'
    when    costCategory = 'outpatient'  and costSubCategory = 'infusion' then 'Infusion'
    when    costCategory = 'outpatient'  and costSubCategory = 'labs' then 'Labs'
    when    costCategory = 'outpatient'  and costSubCategory = 'obs' then 'Observation'
    when    costCategory = 'outpatient'  and costSubCategory = 'radiology' then 'Radiology'
    when    costCategory = 'outpatient'  and costSubCategory = 'surgery' then 'OP Surgery'
    when    costCategory = 'pharmacy'    and costSubCategory = 'pharmacy' and costSubCategoryDetail = 'Brand' then 'RX'
    when    costCategory = 'pharmacy'    and costSubCategory = 'pharmacy' and costSubCategoryDetail = 'Generic' then 'RX'
    when    costCategory = 'pharmacy'    and costSubCategory = 'pharmacy' and (costSubCategoryDetail is null or costSubCategoryDetail not in ('Brand','Generic')) then 'RX'
    when    costCategory = 'professional'    and placeOfService ='65' then 'Dialysis'
    when    costCategory = 'professional'    and costSubCategory = 'ambulance' then 'Transport'
    when    costCategory = 'professional'    and costSubCategory = 'behavioralHealth'  then 'BH'
    when    costCategory = 'professional'    and costSubCategory = 'dialysis'  then 'Dialysis'
    when    costCategory = 'professional'    and costSubCategory = 'dme'  then 'DME'
    when    costCategory = 'professional'    and costSubCategory = 'ed'  then 'ED'
    when    costCategory = 'professional'    and costSubCategory = 'homeHealth'  then 'Home Health'
    when    costCategory = 'professional'    and costSubCategory = 'hospice'  then 'Hospice'
    when    costCategory = 'professional'    and costSubCategory = 'infusion'  then 'Outpatient'
    when    costCategory = 'professional'    and costSubCategory = 'inpatient'  then 'Acute IP'
    when    costCategory = 'professional'    and costSubCategory = 'labs' then 'Labs'
    when    costCategory = 'professional'    and costSubCategory = 'inpatientMaternity'  then 'IP Maternity'
    when    costCategory = 'professional'    and costSubCategory = 'obs' then 'Observation'
    when    costCategory = 'professional'    and placeOfService in ('01','1') then 'Other'
    when    costCategory = 'professional'    and placeOfService ='81' then 'Labs'
    when    costCategory = 'professional'    and placeOfService ='52' then 'OP Psych'
    when    costCategory = 'professional'    and costSubCategory = 'other'  and placeOfService ='11' then 'OV Other'
    when    costCategory = 'professional'    and costSubCategory = 'other'  and (placeOfService !='11' or placeOfService is null) then 'Other'
    when    costCategory = 'professional'    and costSubCategory = 'primaryCare' and costSubCategoryDetail = 'professional' then 'Primary Care'
    when    costCategory = 'professional'    and costSubCategory = 'inpatientPsych' and costSubCategoryDetail = 'professional' then 'IP Psych'
    when    costCategory = 'professional'    and costSubCategory = 'radiology' and costSubCategoryDetail = 'professional' and placeOfService !='11' then 'Radiology'
    when    costCategory = 'professional'    and costSubCategory = 'radiology' and costSubCategoryDetail = 'professional' and placeOfService ='11' then 'Radiology'
    when    costCategory = 'professional'    and costSubCategory = 'rehab' and costSubCategoryDetail = 'professional' then 'IP Rehab'
    when    costCategory = 'professional'    and costSubCategory = 'snf' and costSubCategoryDetail = 'professional' then 'SNF'
    when    costCategory = 'professional'    and costSubCategory = 'specialtyCare' and costSubCategoryDetail = 'professional' then 'Specialty Care'
    when    costCategory = 'professional'    and costSubCategory = 'surgery' and costSubCategoryDetail = 'professional' then 'OP Surgery'
    when    costCategory = 'snf'     and costSubCategory = 'acute' then 'SNF'
    when    costCategory = 'snf'     and costSubCategory = 'subAcute' then 'SNF'
    else null end as costClassification1,


  case
    when    costCategory = 'inpatient'  and costSubCategory = 'maternity' and costSubCategoryDetail = 'childbirth' then 'Facility Delivery'
    when    costCategory = 'inpatient'  and costSubCategory = 'maternity' and costSubCategoryDetail = 'newborn' then 'Facility Newborn'
    when    costCategory = 'inpatient'  and costSubCategory = 'maternity' and costSubCategoryDetail not in ('childbirth', 'newborn') then 'Facility No Delivery'
    when    costCategory = 'inpatient'  and costSubCategory = 'maternity' and costSubCategoryDetail is null then 'Facility No Delivery'
    when    costCategory = 'inpatient'   and costSubCategory = 'rehab' and costSubCAtegoryDetail ='substanceAbuse'  then 'Rehab Facility'
    when    costCategory = 'inpatient'   and costSubCategory = 'rehab' and (costSubCAtegoryDetail !='substanceAbuse' or costSubCategoryDetail is null) then 'Facility'
    when    costCategory = 'inpatient'   and costSubCategory = 'psych' and costSubCAtegoryDetail ='substanceAbuse' then 'Psych Facility'
    when    costCategory = 'inpatient'   and costSubCategory = 'psych' and (costSubCAtegoryDetail !='substanceAbuse' or costSubCategoryDetail is null) then 'Facility'
    when    costCategory = 'inpatient'   and costSubCategory ='acute' and (costSubCategoryDetail != 'icu' or costSubCategoryDetail is null) then 'Facility'
    when    costCategory = 'inpatient'   and costSubCategory ='acute' and costSubCategoryDetail = 'icu' then 'Facility ICU'
    when    costCategory = 'inpatient'   and costSubCategory ='other' then 'Facility other'
    when    costCategory = 'inpatient'  and costSubCategory = 'hospice'  then 'IP Facility'
    when    costCategory = 'outpatient' and (costSubCategory is null or costSubCategory = 'other') then 'Facility'
    when    costCategory = 'outpatient' and costSubCategory = 'ambulance' then 'Ambulance'
    when    costCategory = 'outpatient' and costSubCategory = 'behavioralHealth' then 'Facility'
    when    costCategory = 'outpatient' and costSubCategory =   'dialysis' then 'Facility'
    when    costCategory = 'outpatient' and costSubCategory = 'dme' then 'OP Facility'
    when    costCategory = 'outpatient' and costSubCategory = 'ed' then 'Facility'
    when    costCategory = 'outpatient' and costSubCategory = 'homeHealth' then coalesce( costSubCategoryDetail, 'Other Services')
    when    costCategory = 'outpatient' and costSubCategory = 'hospice' then 'Outpatient'
    when    costCategory = 'outpatient' and costSubCategory = 'infusion' then 'Facility'
    when    costCategory = 'outpatient' and costSubCategory = 'labs' then 'Facility'
    when    costCategory = 'outpatient' and costSubCategory = 'obs' then 'Facility'
    when    costCategory = 'outpatient' and costSubCategory = 'radiology' then 'Facility'
    when    costCategory = 'outpatient' and costSubCategory = 'surgery' then 'Facility'
    when    costCategory = 'pharmacy'   and costSubCategory = 'pharmacy' and costSubCategoryDetail = 'Brand' then 'RX Brand'
    when    costCategory = 'pharmacy'   and costSubCategory = 'pharmacy' and costSubCategoryDetail = 'Generic' then 'RX Generic'
    when    costCategory = 'pharmacy'    and costSubCategory = 'pharmacy' and (costSubCategoryDetail is null or costSubCategoryDetail not in ('Brand','Generic')) then 'RX'
    when    costCategory = 'professional'    and costSubCategory = 'ambulance' and costSubCategoryDetail = 'emergent' then 'Prof - Emerg'
    when    costCategory = 'professional'    and costSubCategory = 'ambulance' and (costSubCategoryDetail != 'emergent' or costSubCategoryDetail is null) then 'Prof - Non Emerg'
    when    costCategory = 'professional'    and costSubCategory in ('behavioralHealth','labs','other') and placeOfService in ('2','02') then 'Professional - Tele'
    when    costCategory = 'professional'    and costSubCategory in ('behavioralHealth','labs','radiology','infusion','dialysis') and placeOfService in ('11') then 'Professional OV'
    when    costCategory = 'professional'    and costSubCategory in ('behavioralHealth','labs') and (placeOfService not in ('2','02','11') or placeOfService is null) then 'Professional'
    when    costCategory = 'professional'    and costSubCategory in ('dme','dialysis') and placeOfService = '12' then 'Home'
    when    costCategory = 'professional'    and costSubCategory in ('infusion','behavioralHealth','surgery') and placeOfService = '12' then 'Professional Home' 
    when    costCategory = 'professional'   and costSubCategory = 'dialysis' and (placeOfService  not in ('11') or placeOfService is null) then 'Professional'
    when    costCategory = 'professional'   and costSubCategory = 'dme'  then 'Professional'
    when    costCategory = 'professional'   and costSubCategory = 'ed' then 'Professional'
    when    costCategory = 'professional'   and costSubCategory = 'homeHealth'  then coalesce( costSubCategoryDetail, 'Other Services') 
    when    costCategory = 'professional'   and costSubCategory = 'hospice'  then 'Professional'
    when    costCategory = 'professional'   and costSubCategory = 'infusion' and placeOfService  not in ('11')  then 'Professional'
    when    costCategory = 'professional'   and costSubCategory = 'inpatient'  then 'Professional'
    when    costCategory = 'professional'   and costSubCategory = 'inpatientMaternity'  then 'Professional'
    when    costCategory = 'professional'   and costSubCategory = 'obs'  then 'Professional'
    when    costCategory = 'professional'   and costSubCategory = 'other' and placeOfService in ('11') then 'Medical'
    when    costCategory = 'professional'   and costSubCategory = 'other' and (placeOfService not in ('2','02','11') or placeOfService is null) then 'Professional'
    when    costCategory = 'professional'   and costSubCategory = 'primaryCare' then 'Professional OV'
    when    costCategory = 'professional'   and costSubCategory = 'inpatientPsych'  then 'Professional'
    when    costCategory = 'professional'   and costSubCategory = 'radiology' and placeOfService  not in ('11') then 'Professional'
    when    costCategory = 'professional'   and costSubCategory = 'rehab' then 'Professional'
    when    costCategory = 'professional'   and costSubCategory = 'snf' then 'Professional'
    when    costCategory = 'professional'   and costSubCategory = 'specialtyCare' and costSubCategoryDetail = 'professional' then 'Professional OV'
    when    costCategory = 'professional'   and costSubCategory = 'surgery' and placeOfService in ('11') then 'Surgical'
    when    costCategory = 'professional'   and costSubCategory = 'surgery' and placeOfService not in ('11') then "Professional"
    when    costCategory = 'snf'    and costSubCategory = 'acute' then 'Facility Acute'
    when    costCategory = 'snf'    and costSubCategory = 'subAcute' then 'Facility Sub-Acute'
    else null end as costClassification2

  from medical_union

  left join {{ ref('ftr_claim_capitation_flags') }} as cap
      on medical_union.claimId = cap.claimId
      and medical_union.lineId = cap.lineId

)

select * from final
