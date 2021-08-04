
------------------------------------
--- Section One: Reference Tables ---
-------------------------------------


--HCC reference data
with hcc_codes as (

select distinct * 
from
{{ source('codesets', 'hcc_2020') }} 

where
lower(cast(hcc_v24_2020 as string)) = 'yes'
),


risk_hcc_descript as (

select distinct 
hcc.*

from 
{{ source('codesets', 'risk_hcc_descript') }} as hcc

where 
version = 'v24'
),


risk_claims_dx as (

select *

from
{{ ref('risk_claims_dx') }} 
where EXTRACT(YEAR FROM evidenceDate) between EXTRACT(YEAR FROM current_date()) - 2 and EXTRACT(YEAR FROM current_date()) 
),


risk_claims_lab as (

select *

from
{{ ref('risk_claims_lab') }}
),


risk_claims_cpt as (

select *

from
{{ ref('risk_claims_cpt') }} 
),


risk_claims_patients1917 as (

select distinct
patientId

from
{{ ref('risk_claims_patients') }}

where 
hcc in ('19','17')
and EXTRACT(YEAR FROM evidenceDate) between EXTRACT(YEAR FROM current_date()) - 2 and EXTRACT(YEAR FROM current_date()) - 1
),


risk_claims_patients_ex18 as (

select distinct
patientId

from
{{ ref('risk_claims_patients') }}

where
hcc = '18'
and EXTRACT(YEAR FROM evidenceDate) between EXTRACT(YEAR FROM current_date()) - 2 and EXTRACT(YEAR FROM current_date()) - 1 
),


--member and lob info
member as (

select distinct
patientId as id, 
memberId,
partner,
lineOfBusiness

from
{{ ref('hcc_risk_members') }}

where
eligYear = extract(Year from current_date())
and eligNow = true
and isHARP = false
and lineOfBusiness <> 'commercial'
),


------------------------------------------------------------------------------------
diabetes_dx_codes as (

select distinct
patientId as patientId,
cast(null as string) as underlyingDxCode,
case when dxCode  in 
        ('Z9842', 'Z9841', 'Z9849', 'H28', 'H26223', 'H26222', 
        'H26221', 'H26229', 'H26213', 'H26212', 'H26211', 'H26219') then 'HIGH'
     
       when dxCode  in (
       'IH26493', 'H26492', 'H26491', 'H26499', 'H268', 
       'H269', 'H2620', 'H2640') then 'MEDIUM'  

       when (dxs.dxCode  ='I670' or dxs.dxCode  like 'I70%' or dxs.dxCode  in (
       select diagnosis_code from hcc_codes where hcc_v24 in ('107','108')
       and diagnosis_code not like '%E%%')) then 'HIGH'  

       when dxCode  in (
       'I70463', 'I70462', 'I70468', 'I70461', 'I70469', 'I70263', 'I70262', 
       'I70268', 'I70261', 'I70269', 'I70563', 'I70562', 'I70568', 'I70561',
       'I70569', 'I70663', 'I70662', 'I70668', 'I70661', 'I70669', 'I70763',
       'I70762', 'I70768', 'I70761', 'I70769', 'I70363', 'I70362', 'I70368',
       'I70361', 'I70369') then 'MEDIUM'

        when dxCode in
        ('N184', 'N185', 'N189', 'N186', 'I129', 'I120', 'I130', 'I132', 'I1310',
        'I1311', 'N183', 'O1022', 'O10211', 'O10212', 'O10213', 'O10219', 'O1023', 'O1032', 
        'O10311', 'O10312', 'O10313', 'O10319', 'O1033') then 'HIGH'

        when dxCode = 'Z8631' then 'HIGH'
        when dxCode = 'I96' then 'HIGH'
        when dxCode in ('E162', 'E161', 'E160') then 'HIGH'
        when dxCode in ('N141', 'N142') then 'MEDIUM'
        when dxCode in ('M129',  'M14679',  'M14671',  'M14672') then 'HIGH'    
        when dxCode in ('G609',  'G608',  'G99', ' G909',  'G64',  'G90', 'G63', 'G629') then 'HIGH'
        when dxCode in ('G59',  'G589') then 'MEDIUM'
        when dxCode = 'K055' then 'MEDIUM'
        when dxCode = 'Z8631' then 'HIGH'

        when dxCode in (
        'K0520',  'K05211',  'K05212', 'K05213',   'K05219',   'K05221',  
        'K05222',  'K05223',  'K05229', 'K0530',  'K05311', 'K05312',  
        'K05313', 'K05319',  'K05321',  'K05322',  'K05323', 'K05329',
        'K054',  'K056',  'K056',  'K08129',  'K08121',  'K08122', 
        'K08123',  'K08422', 'K08423', 'K08424',  'K08429', 'K08421', 
        'K08422', 'K08423', 'K08424', 'K08429','K055') then 'HIGH'

        when dxCode in (
        'D631', 'Z9842', 'Z9841', 'Z9849', 'H28', 'N182', 'N183',  'K08124', 'K08129', 'N186', 
        'K08421', 'K08422', 'K08423',  'K08424', 'K08429', 'Z8631', 'G63','G629', 'O1022', 
        'O10211', 'O10212', 'O10213', 'O10219', 'O1023', 'O1032', 'O10311', 'O10312', 'O10313', 
        'O10319', 'O1033', 'G59', 'G589') then 'HIGH'
        end as confidenceLevel,
dxCode as supportingEvidenceCode,
'ICD' as supportingEvidenceCodeType,
cast(null as string) as supportingEvidenceValue,
case when dxCode  in ( 'Z9842', 'Z9841', 'Z9849', 'H28', 'H26223', 'H26222', 'H26221', 'H26229', 'H26213', 'H26212', 'H26211', 'H26219',
        'IH26493', 'H26492', 'H26491', 'H26499', 'H268', 'H269', 'H2620', 'H2640') then 'Cataracts'  
     when (dxs.dxCode  ='I670' or dxs.dxCode  like 'I70%' or dxs.dxCode  in
     (select diagnosis_code as dxCode from hcc_codes where
     hcc_v24 in ('107','108') and diagnosis_code not like '%E%%')) then 'CAD/CVD' 

      when dxCode  in (
      'I70463', 'I70462', 'I70468', 'I70461', 'I70469', 'I70263', 'I70262', 
      'I70268', 'I70261', 'I70269', 'I70563', 'I70562', 'I70568', 'I70561',
      'I70569', 'I70663', 'I70662', 'I70668', 'I70661', 'I70669', 'I70763',
      'I70762', 'I70768', 'I70761', 'I70769', 'I70363', 'I70362', 'I70368',
      'I70361', 'I70369') then 'Circulatory Condition'

      when dxCode in (
      'N184', 'N185', 'N189', 'N186', 'I129', 'I120', 'I130', 'I132', 'I1310',
      'I1311', 'N183', 'O1022', 'O10211', 'O10212', 'O10213', 'O10219', 'O1023', 'O1032', 
      'O10311', 'O10312', 'O10313', 'O10319', 'O1033') then 'Chronic Kidney Disease'

      when dxCode ='Z8631' then 'Foot Ulcer'
      when dxCode ='I96' then 'Gangrene'
      when dxCode in ('E162', 'E161', 'E160') then 'Hypoglycemia'
      when dxCode in ('N141', 'N142') then 'Nephropathy'
      when dxCode in ('M129',  'M14679',  'M14671',  'M14672') then 'Neuropathic Arthropathy'              
      when dxCode in ('G609',  'G608',  'G99', ' G909',  'G64',  'G90', 'G63', 'G629') then 'Neuropathy'
      when dxCode in ('G59',  'G589') then 'Neuropathy'
      when dxCode = 'Z8631' then 'Periodontal Angiography'

      when dxCode in (
      'K0520',  'K05211',  'K05212', 'K05213',   'K05219',   'K05221',  
      'K05222',  'K05223',  'K05229', 'K0530',  'K05311', 'K05312',  'K05313', 'K05319',  
      'K05321',  'K05322',  'K05323', 'K05329', 'K054',  'K056',  'K056',  'K08129',  'K08121', 
      'K08122', 'K08123',  'K08422', 'K08423', 'K08424',  'K08429', 'K08421', 'K08422', 'K08423', 
      'K08424', 'K08429','K055') then 'Periodontal Disease'


      when dxCode in (
      'D631', 'Z9842', 'Z9841', 'Z9849', 'H28', 'N182', 'N183',  'K08124', 'K08129', 'N186', 
      'K08421', 'K08422', 'K08423',  'K08424', 'K08429', 'Z8631', 'G63','G629', 'O1022', 
      'O10211', 'O10212', 'O10213', 'O10219', 'O1023', 'O1032', 'O10311', 'O10312', 'O10313', 
      'O10319', 'O1033', 'G59', 'G589') then 'Unspecified Conditions'
      end as supportingEvidenceName,

'claims' as  supportingEvidenceSource,
evidenceDate,
current_date() as dateUploaded

from
risk_claims_dx dxs

where 
dxs.dxCode in ( 'Z9842', 'Z9841', 'Z9849', 'H28', 'H26223', 'H26222', 'H26221', 'H26229', 'H26213', 'H26212', 'H26211', 'H26219',
      'IH26493', 'H26492', 'H26491', 'H26499', 'H268', 'H269', 'H2620', 'H2640',

     'I70463', 'I70462', 'I70468', 'I70461', 'I70469', 'I70263', 'I70262', 
     'I70268', 'I70261', 'I70269', 'I70563', 'I70562', 'I70568', 'I70561',
     'I70569', 'I70663', 'I70662', 'I70668', 'I70661', 'I70669', 'I70763',
     'I70762', 'I70768', 'I70761', 'I70769', 'I70363', 'I70362', 'I70368', 
     'I70361', 'I70369',

      'N184', 'N185', 'N189', 'N186', 'I129', 'I120', 'I130', 'I132', 'I1310',
      'I1311', 'N183', 'O1022', 'O10211', 'O10212', 'O10213', 'O10219', 'O1023', 'O1032', 
      'O10311', 'O10312', 'O10313', 'O10319', 'O1033',

      'Z8631',
           
      'I96',
      
      'E162', 'E161', 'E160',

      'N141', 'N142',

      'M129',  'M14679',  'M14671',  'M14672',

      'G609',  'G608',  'G99', ' G909',  'G64',  'G90', 'G63', 'G629',

      'G59',  'G589',

      'K0520',  'K05211',  'K05212', 'K05213',   'K05219',   'K05221',  
      'K05222',  'K05223',  'K05229', 'K0530',  'K05311', 'K05312',  'K05313', 'K05319',  
      'K05321',  'K05322',  'K05323', 'K05329', 'K054',  'K056',  'K056',  'K08129',  'K08121', 
      'K08122', 'K08123',  'K08422', 'K08423', 'K08424',  
      'K08429', 'K08421', 'K08422', 'K08423', 'K08424', 'K08429','K055',

       'Z8631' ,

      'D631', 'Z9842', 'Z9841', 'Z9849', 'H28', 'N182', 'N183',  'K08124', 'K08129', 'N186', 
      'K08421', 'K08422', 'K08423',  'K08424', 'K08429', 'Z8631', 'G63','G629', 'O1022', 
      'O10211', 'O10212', 'O10213', 'O10219', 'O1023', 'O1032', 'O10311', 'O10312', 'O10313', 
      'O10319', 'O1033', 'G59', 'G589')

or

(dxs.dxCode  ='I670' or dxs.dxCode  like 'I70%' or dxs.dxCode  in
     (select diagnosis_code from hcc_codes where
     hcc_v24 in ('107','108') and diagnosis_code not like '%E%%')) 
),


diabetes_lab_test as (

select distinct   
patientId,
cast(null as string) as underlyingDxCode,
'HIGH' as confidenceLevel,
loinc as supportingEvidenceCode,
'LOINC' as supportingEvidenceCodeType,
cast(resultNumeric as string) as supportingEvidenceValue,
case when (cast( REGEXP_REPLACE(loinc,'[^0-9 ]','') as string) = '322941' ) then 'diabetic-comp-neph'
     when (cast( REGEXP_REPLACE(loinc,'[^0-9 ]','') as string) = '137059' ) then 'diabetic-comp-neph'
     when (cast( REGEXP_REPLACE(loinc,'[^0-9 ]','') as string) = '10155' ) then 'diabetic-comp-neph'
     end as supportingEvidenceName,
'lab' as supportingEvidenceSource,
evidenceDate,
current_date()  as dateUploaded

from
risk_claims_lab

where       
((trim(cast( REGEXP_REPLACE(loinc,'[^0-9 ]','') as string)) ='322941' and resultNumeric >300) 
or (trim(cast( REGEXP_REPLACE(loinc,'[^0-9 ]','') as string)) ='137059' and resultNumeric >200) 
or (trim(cast( REGEXP_REPLACE(loinc,'[^0-9 ]','') as string)) ='10155' and resultNumeric >300) 
)          
),


diabetes_cpt_codes as (

select distinct
patientId,
cast(null as string) as underlyingDxCode,
case when code  in('3060F' ,'3062F' ) then 'MEDIUM' else 'HIGH' end as ConfidenceLevel,
code as supportingEvidenceCode,
codeset as supportingEvidenceCodeType,
cast(null as string) as supportingEvidenceValue,
case when code in ('93922',  '93923',  '93924') then 'Cataract'
     when code in ('3075F','3077F', '3079F','3080F') then 'Circulatory Disease - BP >130/80'
     when code = '3046F' then 'Hypoglycemia - Most recent hemoglobin A1c (HbA1c) level > 9.0%'
     when code in ('3060F','3062F') then 'Nephropathy - Positive microalbuminuria test result documented and reviewed'
     when code between '90935' and '90940' then 'CKD - Hemodialysis Procedures'
     when code between '90945' and '90947' then 'CKD - Miscellaneous Dialysis Services and Procedures'
     when code between '90951' and '90970' then 'CKD - End-Stage Renal Disease Services'
     when code between '90989' and '90999' then 'CKD - Other Dialysis Procedures'
     end as supportingEvidenceName,

'claims' as supportingEvidenceSource,
evidenceDate,
current_date()  as dateUploaded

from
risk_claims_cpt clms

where 
code in 
('93922',  '93923',  '93924','3075F','3077F', '3079F','3080F','3046F', '3060F','3062F')
or
code between '90935' and '90940'
or 
code between '90945' and '90947'
or 
code between '90951' and '90970'
or 
code between '90989' and '90999'
),


diabetes_suspects_mem as (

select distinct
p.partner, 
dxs.patientId, 
p.lineOfBusiness, 
'SUSP' as conditionType,
'internal_rules' as condtionSource,
case when lower(p.lineOfBusiness) = 'medicaid' and partner <> 'emblem' then 'CDPS'
     when lower(p.lineOfBusiness) = 'medicaid' and partner = 'emblem' then '3M CRG'
     else 'HCC'
           end as ConditionCategory,

case when lower(p.lineOfBusiness) = 'medicaid' and partner <> 'emblem' then 'DIA2M'
     when lower(p.lineOfBusiness) = 'medicaid' and partner = 'emblem' then '428'
     else '18'
           end as conditionCategoryCode,

case when lower(p.lineOfBusiness) = 'medicaid' and partner = 'emblem' then 'Diabetes with Circulatory Complication'
     when lower(p.lineOfBusiness) =  'medicaid' and partner <> 'emblem' then 'Type 2 or unspecified diabetes with complications, proliferative diabetic retinopathy'
     else 'Diabetes with Chronic Complications'
           end as ConditionName,
case when riskex.patientID is null then 'OPEN' 
     else 'CLOSED' 
          end as ConditionStatus,
underlyingDxCode,
confidenceLevel, 
case when upper(confidenceLevel) = 'VERY HIGH' then 1
     when upper(confidenceLevel) = 'HIGH' then 2
     when upper(confidenceLevel) = 'MEDIUM' then 3
     when upper(confidenceLevel) = 'LOW' then 4
     when upper(confidenceLevel) = 'VERY LOW' then 5
          else 5 end as confidenceNumeric,
supportingEvidenceCode,
supportingEvidenceCodeType, 
supportingEvidenceValue, 
supportingEvidenceName, 
supportingEvidenceSource, 
dateUploaded,
cast(null as string) as providerFirstName, 
cast(null as string) as providerLastNameLegalName,
count(distinct dxs.evidenceDate) as clmCount,
max(dxs.evidenceDate) as clmLatest

from
(select * from diabetes_lab_test
union all
select * from diabetes_dx_codes
union all
select * from diabetes_cpt_codes
) dxs

INNER JOIN
member p
on dxs.patientID = p.id 

INNER JOIN 
risk_claims_patients1917 risk
on dxs.patientID = risk.PatientID

left join 
risk_claims_patients_ex18 riskex
on dxs.patientID = riskex.PatientID

group by  
p.partner, 
dxs.patientId, 
p.lineOfBusiness, 
conditionCategory, 
conditionStatus,
underlyingDxCode, 
confidenceLevel, 
supportingEvidenceCode,
supportingEvidenceCodeType,  
supportingEvidenceValue, 
supportingEvidenceName, 
supportingEvidenceSource,
dateUploaded
)

--final
select distinct * from diabetes_suspects_mem
