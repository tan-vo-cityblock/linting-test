with hcc_risk_members as (

  select distinct 
  eligYear,
  monthcount,
  patientid, 
  firstname, 
  lastname, 
  dateofbirth, 
  medicareId, 
  memberID,
  cohortname,
  cohortgolivedate,
  currentState,
  virtualProgram,
  lineofbusiness

  from 
  {{ref('hcc_risk_members')}}

  where 
  lineofbusiness not in ( 'medicaid', 'commercial')
),


connMMR as (

select *, 
'connecticare' as payer,
'dsnp' as lineofbusiness

from 
{{source('cci_cms_revenue','mmr_monmemd_3276_*')}}
),


NMI_MBI_Crosswalk as (

select distinct
*
from
{{source('Settlement_CCI','NMI_MBI_Crosswalk')}}
),


conndata as (
  select distinct
  eligyear,
  payer,
  mmr.lineofbusiness,
  xw.PATIENTID AS PATIENTID,
  firstname,
  lastname,
  dateofbirth,
  mmt.cohortname,
  mmt.cohortgolivedate,
  mmt.virtualProgram,
  mmt.currentState,
  mmt.monthCount,
  Payment_Date,
  trim(Beneficiary_Id) as Beneficiary_Id ,
  mmr.gender,
  cast(ESRD as string) as ESRD,
  HOSPICE,
  Institutional,
  replace(cast(Risk_Adjustment_Age_Group_RAAG as string),'-','')  as Risk_Adjustment_Age_Group_RAAG,
  trim(Risk_Adjustment_Factor_Type_Code)  as Risk_Adjustment_Factor_Type_Code ,
  cast(Original_Reason_for_Entitlement_Code_OREC as string) AS Original_Reason_for_Entitlement_Code_OREC, 
  case when trim(Risk_Adjustment_Factor_Type_Code)  in ('C', 'CF', 'CN', 'CP', 'I','I1', 'I2') then 0 
       when trim(Risk_Adjustment_Factor_Type_Code)  in ('E') then 1
       when trim(Risk_Adjustment_Factor_Type_Code)  in ('SE') then 3
       when trim(Risk_Adjustment_Factor_Type_Code)  in ('D', 'C1', 'C2','G1', 'G2') then 4
       when trim(Risk_Adjustment_Factor_Type_Code)  in ('ED', 'E1', 'E2') then 5
       END AS type,
       case when cast(plan_benefit_package_id as INT64) >= 800 then true
        else false
            end as EGWP
  from 
  connMMR mmr

  left join
  NMI_MBI_Crosswalk xw
  ON trim(mmr.Beneficiary_Id) = trim(xw.MBI)

  left join
  hcc_risk_members mmt
  on upper(trim(xw.patientID)) = upper(trim(mmt.patientID ))
  and  extract(year from Payment_Date ) = eligyear
  and mmr.lineofbusiness = mmt.lineofbusiness
  )

  select * from conndata