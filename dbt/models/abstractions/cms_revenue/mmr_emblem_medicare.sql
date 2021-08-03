
with masterMember as (

select distinct 
eligYear,
monthcount,
patientid, 
firstname, 
lastname, 
gender,
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

emblemMMR as (

select 
patient.patientid,
data.First_Name as firstname,
data.Surname as lastname, 
'emblem' as payer,
data.ESRD as ESRD, 
data.Hospice as HOSPICE, 
data.HIC_Number as MBI,
cast(data.Original_Reason_Entitlement_Cd as string) AS Original_Reason_for_Entitlement_Code_OREC, 
case when data.Institutional = "Y" then 'Y' else cast(null as string) end as LTI_Flag, 
data.RA_Factor_Type_Code as RA_Factor_Type_Code,
cast(data.Risk_Adj_Age_Group as string) as Risk_Adj_Age_Group,
parse_date('%m%d%Y',(concat(substring(cast(data.Report_date as string),5,6),'01',substring(cast(data.Report_date as string),1,4)))) as Payment_Date,
case when cast(data.Plan_Benefit_Package_Id as INT64) >= 800 then true
        else false
            end as EGWP
FROM
{{source('emblem_revenue','MMR_20*')}}

where 
data.Report_date is not null
),


emblemdata as (
select distinct
eligyear,
payer, 
mmt.lineofbusiness,
mmr.patientID,
mmr.firstname,
mmr.lastname, 
mmt.dateofbirth  as date_of_birth,
cohortname, 
cohortgolivedate,
mmt.virtualProgram,
mmt.currentState,
mmt.monthCount,
Payment_Date,
MBI,
gender,
ESRD, 
HOSPICE, 
LTI_Flag, 
Risk_Adj_Age_Group,
trim(RA_Factor_Type_Code) as RA_Factor_Type_Code, 
Original_Reason_for_Entitlement_Code_OREC,
case when trim(RA_Factor_Type_Code) in ('C', 'CF', 'CN', 'CP', 'I','I1', 'I2') then 0 
     when trim(RA_Factor_Type_Code) in ('E') then 1
     when trim(RA_Factor_Type_Code) in ('SE') then 3
     when trim(RA_Factor_Type_Code) in ('D','C1', 'C2','G1', 'G2') then 4
     when trim(RA_Factor_Type_Code) in ('ED', 'E1', 'E2') then 5
     END AS type,
     EGWP

from 
emblemMMR mmr

left join 
masterMember mmt
on mmr.patientId  = mmt.patientId
and  extract(year from Payment_Date) = eligyear 
)


select * from emblemdata