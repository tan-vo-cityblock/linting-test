with masterMember as (

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


tuftsMMR as (

select distinct
'tufts' as payer,
'duals' as lineofbusiness,
date_of_birth,
Payment_Date,
Beneficiary_Id as MBI,
First_Initial,
Surname,
gender,
cast(ESRD as string) as ESRD,
HOSPICE,
LTI_Flag,
replace(replace(cast(Risk_Adjustment_Age_Group_RAAG as string),"-",""),"00","") as Risk_Adj_Age_Group, 
cast(Original_Reason_for_Entitlement_Code_OREC as string) as Original_Reason_for_Entitlement_Code_OREC, 
Risk_Adjustment_Factor_Type_Code,
case when cast(Plan_Benefit_Package_Id as INT64) >= 800 then true
        else false
            end as EGWP

from 
{{source('tufts_revenue','mmr_monmemd_7419_*')}}
),


tuftsMMR2 as (

select distinct
'tufts' as payer,
'duals' as lineofbusiness,
PARSE_DATE('%Y%m%d',  date_of_birth) as date_of_birth,
PARSE_DATE('%Y%m%d',  concat(Payment_Date,"01")) as Payment_Date ,
Beneficiary_Id as MBI,
First_Initial,
Surname,
gender_code,
cast(ESRD as string) as ESRD,
HOSPICE,
LTI_Flag,
replace(replace(cast(Risk_Adjustment_Age_Group as string),"-",""),"00","") as Risk_Adj_Age_Group,
cast(OREC as string) as Original_Reason_for_Entitlement_Code_OREC, 
Risk_Adjustment_Factor_Type_Code,
case when cast(Plan_Benefit_Package_Id as INT64) >= 800 then true
        else false
            end as EGWP

from
 `cityblock-analytics.abs_risk.tufts_mmr_monthly`
 where Payment_Date is not null and Payment_Date <> ""

 ),


mmr as (

select * from tuftsMMR
union distinct
select * from tuftsMMR2
),


tuftsdata as (
  
select distinct
eligYear,
payer, 
mmr.lineofbusiness,
patientID,
firstname,
lastname, 
date_of_birth,
cohortname, 
cohortgolivedate,
mmt.virtualProgram,
mmt.currentState,
mmt.monthcount,
Payment_Date,
MBI,
mmr.gender,
ESRD, 
HOSPICE,
LTI_Flag, 
Risk_Adj_Age_Group,
trim(Risk_Adjustment_Factor_Type_Code) as Risk_Adjustment_Factor_Type_Code, 
Original_Reason_for_Entitlement_Code_OREC, 
case when Risk_Adjustment_Factor_Type_Code in ('C', 'CF', 'CN', 'CP', 'I','I1', 'I2') then 0
     when Risk_Adjustment_Factor_Type_Code in ('E') then 1
     when Risk_Adjustment_Factor_Type_Code in ('SE') then 3
     when Risk_Adjustment_Factor_Type_Code in ('D', 'C1', 'C2','G1', 'G2') then 4
     when Risk_Adjustment_Factor_Type_Code in ('ED', 'E1', 'E2') then 5
     END AS type ,
     EGWP
from 
mmr

left join 
masterMember mmt
on upper(substr(trim(mmr.First_Initial),0,1)) = upper(substr(trim( firstname),0,1 ))
and upper(substr(trim(mmr.Surname),0,6)) = upper(substr(trim( lastname ),0,6))
and mmr.date_of_birth  =  dateofbirth
and eligyear = extract(year from Payment_Date)
and mmr.lineofbusiness = mmt.lineofbusiness
)

select * from tuftsdata