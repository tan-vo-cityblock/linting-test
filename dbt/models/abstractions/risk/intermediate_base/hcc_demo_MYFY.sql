
with emblemMMR as (

select * from {{ref('mmr_emblem_medicare')}}
),


connDsnpMMR as (

select * from {{ref('mmr_conn_dsnp')}}
),


connMedicareMMR as (

select * from {{ref('mmr_conn_medicare')}}
),


tuftsMMR as (

select * from {{ref('mmr_tufts_duals')}}
),


DemoFactorsMedAdvan as (

select distinct * from {{source('codesets','DemoFactorsMedAdvan')}}
),


mmr as (

select * from emblemMMR
union distinct
select * from tuftsMMR
union distinct
select * from connDsnpMMR
union distinct
select * from connMedicareMMR
),


hospiceStatus as (

select distinct
eligyear,
patientID as hosppat, 
true as hospiceStatus

from
mmr

where 
upper(hospice) = 'Y'
),


esrdStatus as (

select distinct 
eligyear,
patientID as esrdpat, 
true as esrdstatus

from
mmr

where 
upper(esrd) = 'Y'
),


instStatus as (
select distinct 
eligyear,
patientID as instpat, 
true as institutionalStatus

from
mmr

where 
upper(LTI_Flag) = 'Y'
),


disenrolled as (

SELECT distinct
patientId as disID,
extract (year from disenrolledAt) as disYear

FROM 
 {{ref('member_states')}} 

where
disenrolledAt is not null
),


demoraw as (

 SELECT distinct 
 mmr.* ,
  case 
  when trim(RA_Factor_Type_Code) = 'CF' and cast(Original_Reason_for_Entitlement_Code_OREC as STRING) in ('0','9') then CFA
  when trim(RA_Factor_Type_Code) = 'CF' and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) ) >=6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then CFA
  when trim(RA_Factor_Type_Code) = 'CF' and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) ) <6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then CFD
 
  when trim(RA_Factor_Type_Code) = 'CN' and cast(Original_Reason_for_Entitlement_Code_OREC as STRING) in ('0','9') then CNA
  when trim(RA_Factor_Type_Code) = 'CN' and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) ) >=6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then CNA
  when trim(RA_Factor_Type_Code) = 'CN' and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) ) <6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then CND
 
  when trim(RA_Factor_Type_Code) = 'CP' and cast(Original_Reason_for_Entitlement_Code_OREC as STRING) in ('0','9') then CPA
  when trim(RA_Factor_Type_Code) = 'CP' and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) ) >=6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then CPA
  when trim(RA_Factor_Type_Code) = 'CP' and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) ) <6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then CPD
 
  when trim(RA_Factor_Type_Code) = 'D' and cast(Original_Reason_for_Entitlement_Code_OREC as STRING) in ('0','9') then CFA
  when trim(RA_Factor_Type_Code) = 'D' and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) ) >=6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then CFA
  when trim(RA_Factor_Type_Code) = 'D' and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) ) <6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then CFD
 
  when trim(RA_Factor_Type_Code) in ('I','I1', 'I2')  then  safe_cast(codeset.Institutional as numeric)
 
  when trim(RA_Factor_Type_Code) in ('E','ED') and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) ) >=6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then CFA  
  when trim(RA_Factor_Type_Code) in ('E','ED')  and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) ) <6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then CFD  
  when trim(RA_Factor_Type_Code) in ('E','ED')  and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('0','9') then CFA 
  end as Coefficient, 
 
  case 
  when trim(RA_Factor_Type_Code) = 'CF' and cast(Original_Reason_for_Entitlement_Code_OREC as STRING) in ('0','9') then 'CFA'
  when trim(RA_Factor_Type_Code) = 'CF' and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) )  >=6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then 'CFA'
  when trim(RA_Factor_Type_Code) = 'CF' and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) )  <6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then 'CFD'
 
  when trim(RA_Factor_Type_Code) = 'CN' and cast(Original_Reason_for_Entitlement_Code_OREC as STRING) in ('0','9') then 'CNA'
  when trim(RA_Factor_Type_Code) = 'CN' and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) )  >=6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then 'CNA'
  when trim(RA_Factor_Type_Code) = 'CN' and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) )  <6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then 'CND'
 
 when trim(RA_Factor_Type_Code) = 'CP' and cast(Original_Reason_for_Entitlement_Code_OREC as STRING) in ('0','9') then 'CPA'
 when trim(RA_Factor_Type_Code) = 'CP' and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) )  >=6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then 'CPA'
 when trim(RA_Factor_Type_Code) = 'CP' and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) )  <6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then 'CPD'

 when trim(RA_Factor_Type_Code) = 'D' and cast(Original_Reason_for_Entitlement_Code_OREC as STRING) in ('0','9') then 'CFA'
 when trim(RA_Factor_Type_Code) = 'D' and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) )  >=6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then 'CFA'
 when trim(RA_Factor_Type_Code) = 'D' and   ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) )  <6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then 'CFD'

 when trim(RA_Factor_Type_Code) in ('I','I1', 'I2')  then  'I' 

 when trim(RA_Factor_Type_Code) in ('E','ED') and ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) )  >=6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then 'CFA'  
 when trim(RA_Factor_Type_Code) in ('E','ED') and ifnull(agegroup, safe_cast(Risk_Adj_Age_Group as numeric) )  <6065 and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('1','2','3') then 'CFD'  
 when trim(RA_Factor_Type_Code) in ('E','ED') and cast(Original_Reason_for_Entitlement_Code_OREC as string) in ('0','9') then 'CFA' 
 end as coefficientCategory, 
 case when trim(RA_Factor_Type_Code) in ("E", "ED", "E1", "E2", "SE") then true else false end as newEnrollee,

 codeset.agegroup, 
 codeset.new as newtype

 FROM 
 mmr

 left join
 DemoFactorsMedAdvan codeset
 on  trim(mmr.gender) = codeset.sex
 and (mmr.eligyear) = codeset.Year
 and trim(mmr.Risk_Adj_Age_Group) = trim(cast(codeset.ageGroup as string))
 and mmr.type  = codeset.new 
 ),


 --Including patient Identifting info creating demo temp
 demo as (
 select distinct 
 * except (payment_date)
 from
 (select distinct 
 d.eligYear,
 payer as partner,  
 d.patientID, 
 coefficientCategory, 
 trim(RA_Factor_Type_Code) as RA_Factor_Type_Code, 
 coefficient,
 payment_date, 
 lineOfBusiness, 
 substring(firstname, 1,1) as firstInitial,
 lastname, 
 case when esrdstatus.esrdstatus = true then true else false end as esrdstatus,
 case when hospicestatus.hospicestatus = true then true else false end as hospicestatus,
 case when instStatus.institutionalStatus = true then true else false end as institutionalStatus,
 gender, 
 date_of_birth, 
 DATE_DIFF(Payment_Date,date_of_birth, YEAR)
- 
IF(EXTRACT(MONTH FROM date_of_birth)*100 + EXTRACT(DAY FROM date_of_birth) > EXTRACT(MONTH FROM Payment_Date)*100 + EXTRACT(DAY FROM Payment_Date),1,0) AS age,
 cohortname, 
 cohortgolivedate, 
 currentstate, 
 virtualprogram,
 monthCount,
 newEnrollee,
 case when disYear is null then false else true end as disenrolledYear,
 Original_Reason_for_Entitlement_Code_OREC,
 dense_rank() over (partition by d.eligYear, patientId order by case when coefficientCategory is null then parse_date('%m/%d/%Y','01/01/2019') else payment_date end desc, RA_Factor_Type_Code, ageGroup desc, Original_Reason_for_Entitlement_Code_OREC) as ranked
 
 from
 demoraw d 
 
 left join
 disenrolled
 on patientId = disID
 and d.eligYear = disYear

 left join
 esrdStatus
 on d.patientID = esrdStatus.esrdpat
 and d.eligYear = esrdStatus.eligYear

 left join
 hospiceStatus
 on d.patientID = hospiceStatus.hosppat
 and d.eligYear = hospiceStatus.eligYear

 left join
 instStatus
 on d.patientID = instStatus.instpat
 and d.eligYear >= instStatus.eligYear
 )
 
 where ranked = 1
 )

select * from demo
where eligYear = 2019
