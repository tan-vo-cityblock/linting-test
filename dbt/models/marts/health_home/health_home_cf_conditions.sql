-- health home member computed field conditions
-- author: nabig chaudhry

-- cte 1: obtaining member health home enrollment and status info
with hh_members as (
  select patientId
  from {{ref( 'health_home_tracking_report' )}} ),

-- cte 2: obtaining health home specific member conditions from computed fields
computed_field_conditions as (
  select patientId,
  case 
    when fieldSlug = 'recent-outpatient-mh' and fieldValue = 'true' then true
    when fieldSlug = 'recent-inpatient-mh' and fieldValue = 'true' then true
    else false
    end as healthHomeComputedMentalHealth,
  case
    when fieldSlug = 'suspect-drug-abuse' and fieldValue = 'true' then true
    when fieldSlug = 'history-of-opiates' and fieldValue = 'true' then true
    when fieldSlug = 'history-of-alcohol' and fieldValue = 'true' then true
    when fieldSlug = 'suspect-alcohol' and fieldValue = 'true' then true
    when fieldSlug = 'history-of-other-substances' and fieldValue = 'true' then true
    when fieldSlug = 'rx-opioids-multiple-providers' and fieldValue = 'true' then true
    else false
    end as healthHomeComputedSubstanceAbuse,
  case
    when fieldSlug = 'asthma' and fieldValue = 'true' then true
    when fieldSlug = 'on-inhaler' and fieldValue = 'true' then true
    when fieldSlug = 'oxygen' and fieldValue = 'true' then true
    else false
    end as healthHomeComputedAsthma,
  case
    when fieldSlug = 'on-diabetes-medications' and fieldValue = 'true' then true
    when fieldSlug = 'suspect-diabetes-screening' and fieldValue = 'true' then true
    when fieldSlug = 'diabetes-a-1-c' and fieldValue = 'need-a-1-c' then true
    when fieldSlug = 'diabetes-a-1-c' and fieldValue = 'stable' then true
    when fieldSlug = 'diabetes-a-1-c' and fieldValue = 'mild' then true
    when fieldSlug = 'diabetes-a-1-c' and fieldValue = 'moderate' then true
    when fieldSlug = 'diabetes-a-1-c' and fieldValue = 'severe' then true
    when fieldSlug = 'has-diabetes' and fieldValue = 'true' then true
    when fieldSlug = 'diabetes-poorly-controlled' and fieldValue = 'true' then true
    when fieldSlug = 'suspect-diabetes' and fieldValue = 'true' then true
    when fieldSlug = 'diabetes-on-statin' and fieldValue = 'true' then true
    when fieldSlug = 'high-hemoglobin-a1c' and fieldValue = 'true' then true
    when fieldSlug = 'a-1-c-lab' and fieldValue = 'true' then true
    when fieldSlug = 'a-1-c-level' and fieldValue = 'mild' then true
    when fieldSlug = 'a-1-c-level' and fieldValue = 'moderate' then true
    when fieldSlug = 'a-1-c-level' and fieldValue = 'severe' then true
    when fieldSlug = 'needs-a-1-c' and fieldValue = 'true' then true
    when fieldSlug = 'a-1-c-proc' and fieldValue = 'true' then true
    when fieldSlug = 'check-a-1-c' and fieldValue = 'true' then true
    when fieldSlug = 'on-insulin' and fieldValue = 'true' then true
    when fieldSlug = 'dm-adequately-controlled' and fieldValue = 'true' then true
    when fieldSlug = 'dm-moderately-controlled' and fieldValue = 'true' then true
    when fieldSlug = 'dm-needs-nephropathy-screen' and fieldValue = 'true' then true
    when fieldSlug = 'nephropathy-treatment' and fieldValue = 'true' then true
    else false
    end as healthHomeComputedDiabetes,
  case
    when fieldSlug = 'heart-failure' and fieldValue = 'true' then true
    when fieldSlug = 'cvd' and fieldValue = 'true' then true
    when fieldSlug = 'cvd-on-high-mod-statin' and fieldValue = 'true' then true
    when fieldSlug = 'cabg' and fieldValue = 'true' then true
    when fieldSlug = 'pci' and fieldValue = 'true' then true
    when fieldSlug = 'ivd' and fieldValue = 'true' then true
    when fieldSlug = 'other-revascularization' and fieldValue = 'true' then true
    when fieldSlug = 'acute-mi' and fieldValue = 'true' then true
    when fieldSlug = 'mi' and fieldValue = 'true' then true
    when fieldSlug = 'angina' and fieldValue = 'true' then true
    else false
    end as healthHomeComputedHeartDisease,
  case
    when fieldSlug = 'obesity' and fieldValue = 'true' then true
    else false
    end as healthHomeComputedOverweight,
  case 
    when fieldSlug = 'hiv'and fieldValue = 'true' then true
    else false
    end as healthHomeComputedHiv,
  false as healthHomeComputedComplexTrauma,
  case 
    when fieldSlug = 'ptsd' and fieldValue = 'true' then true
    when fieldSlug = 'schizophrenia' and fieldValue = 'true' then true
    when fieldSlug = 'personality-disorder' and fieldValue = 'true' then true
    when fieldSlug = 'has-anxiety-diagnosis' and fieldValue = 'true' then true
    when fieldSlug = 'suspect-depression' and fieldValue = 'true' then true
    when fieldSlug = 'depression' and fieldValue = 'true' then true
    when fieldSlug = 'on-psychiatric-medication' and fieldValue = 'true' then true
    when fieldSlug = 'history-of-psychosis' and fieldValue = 'true' then true
    when fieldSlug = 'bipolar-disorder' and fieldValue = 'true' then true
    when fieldSlug = 'eating-disorder' and fieldValue = 'true' then true
    else false
    end as healthHomeComputedSeriousMentalIllness,
  case 
    when fieldSlug = 'epilepsy' and fieldValue = 'true' then true
    else false
    end as healthHomeComputedDevelopmentalDisabilities,
  false as healthHomeComputedChildrenHCBSOnly,
  false as healthHomeComputedChildrenHCBSOther,
  false as healthHomeComputedAdultHCBSOther,
  case 
    when fieldSlug = 'esrd' and fieldValue = 'true' then true
    when fieldSlug = 'esrd-current' and fieldValue = 'true' then true
    when fieldSlug = 'on-dialysis' and fieldValue = 'true' then true
    when fieldSlug = 'kidney-transplant' and fieldValue = 'true' then true
    when fieldSlug = 'ckd-stage-4' and fieldValue = 'true' then true
    when fieldSlug = 'on-chronic-opioids' and fieldValue = 'true' then true
    when fieldSlug = 'dementia' and fieldValue = 'true' then true
    when fieldSlug = 'cirrhosis' and fieldValue = 'true' then true
    when fieldSlug = 'muscular-pain-and-disease' and fieldValue = 'true' then true
    when fieldSlug = 'viral-hepatitis' and fieldValue = 'true' then true
    when fieldSlug = 'hypertension' and fieldValue = 'true' then true
    when fieldSlug = 'ace-arbs' and fieldValue = 'true' then true
    when fieldSlug = 'elevated-historical-bp' and fieldValue = 'true' then true
    when fieldSlug = 'bp-presence' and fieldValue = 'true' then true
    when fieldSlug = 'hypertension-admission-4-month' and fieldValue = 'true' then true
    when fieldSlug = 'cancer' and fieldValue = 'true' then true
    when fieldSlug = 'estrogen-agonists' and fieldValue = 'true' then true
    when fieldSlug = 'stroke' and fieldValue = 'true' then true
    when fieldSlug = 'osteoporosis' and fieldValue = 'true' then true
    else false
    end as healthHomeComputedOther,
  case 
    when fieldSlug = 'esrd' and fieldValue = 'true' then 'kidney failure'
    when fieldSlug = 'esrd-current' and fieldValue = 'true' then 'kidney failure'
    when fieldSlug = 'on-dialysis' and fieldValue = 'true' then 'kidney failure'
    when fieldSlug = 'kidney-transplant' and fieldValue = 'true' then 'kidney failure'
    when fieldSlug = 'ckd-stage-4' and fieldValue = 'true' then 'kidney failure'
    when fieldSlug = 'on-chronic-opioids' and fieldValue = 'true' then 'chronic pain'
    when fieldSlug = 'dementia' and fieldValue = 'true' then 'dementia'
    when fieldSlug = 'cirrhosis' and fieldValue = 'true' then 'cirrhosis'
    when fieldSlug = 'muscular-pain-and-disease' and fieldValue = 'true' then 'muscular pain and disease'
    when fieldSlug = 'viral-hepatitis' and fieldValue = 'true' then 'liver inflammation and damage'
    when fieldSlug = 'hypertension' and fieldValue = 'true' then 'hypertension'
    when fieldSlug = 'ace-arbs' and fieldValue = 'true' then 'hypertension'
    when fieldSlug = 'elevated-historical-bp' and fieldValue = 'true' then 'hypertension'
    when fieldSlug = 'bp-presence' and fieldValue = 'true' then 'hypertension'
    when fieldSlug = 'hypertension-admission-4-month' and fieldValue = 'true' then 'hypertension'
    when fieldSlug = 'cancer' and fieldValue = 'true' then 'cancer'
    when fieldSlug = 'estrogen-agonists' and fieldValue = 'true' then 'cancer'
    when fieldSlug = 'stroke' and fieldValue = 'true' then 'stroke'
    when fieldSlug = 'osteoporosis' and fieldValue = 'true' then 'osteoporosis'
    else null
    end as healthHomeComputedOtherDescription,
  from {{ ref('all_computed_fields') }} ), 

-- cte 3: joining together and creating health home member conditions table
final as (
  select h.patientId,
  max(healthHomeComputedMentalHealth) as healthHomeComputedMentalHealth,
  max(healthHomeComputedSubstanceAbuse) as healthHomeComputedSubstanceAbuse,
  max(healthHomeComputedAsthma) as healthHomeComputedAsthma,
  max(healthHomeComputedDiabetes) as healthHomeComputedDiabetes,
  max(healthHomeComputedHeartDisease) as healthHomeComputedHeartDisease,
  max(healthHomeComputedOverweight) as healthHomeComputedOverweight, 
  max(healthHomeComputedHiv) as healthHomeComputedHiv, 
  max(healthHomeComputedComplexTrauma) as healthHomeComputedComplexTrauma, 
  max(healthHomeComputedSeriousMentalIllness) as healthHomeComputedSeriousMentalIllness, 
  max(healthHomeComputedDevelopmentalDisabilities) as healthHomeComputedDevelopmentalDisabilities,
  max(healthHomeComputedChildrenHCBSOnly) as healthHomeComputedChildrenHCBSOnly, 
  max(healthHomeComputedChildrenHCBSOther) as healthHomeComputedChildrenHCBSOther,
  max(healthHomeComputedAdultHCBSOther) as healthHomeComputedAdultHCBSOther,
  max(healthHomeComputedOther) as healthHomeComputedOther, 
  max(healthHomeComputedOtherDescription) as healthHomeComputedOtherDescription
  from hh_members h
  inner join computed_field_conditions p
  on h.patientId = p.patientId
  group by h.patientId )

-- final query
select * from final