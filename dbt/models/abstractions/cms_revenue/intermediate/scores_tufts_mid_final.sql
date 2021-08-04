
with masterMember as (

select distinct
eligYear,
patientid,
firstname,
lastname,
dateofbirth

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
Beneficiary_Id,
mmr_adjustment_reason_code,
Risk_Adjustment_Factor_A,
Number_of_Payment_Adjustment_Months_Part_A as Months_Adj_Part_A,
rpad(replace(cast(Payment_Adjustment_Start_Date as string),"-",""),8, "01")  as PAYMENTSTARTDT,
rpad(replace(cast(Payment_Adjustment_End_Date as string),"-",""),8, "01")  as PAYMENTENDDT

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
Beneficiary_ID,
Adjustment_Reason_Code,
cast(Risk_Adjustment_Factor_A as numeric) as Risk_Adjustment_Factor_A,
cast(Number_of_Payment_Adjust_Months_Part_A as int64) as Number_of_Payment_Adjust_Months_Part_A,
rpad(replace(Payment_Adjust_Start_Date,"-",""),8, "01")  as PAYMENTSTARTDT,
rpad(replace(Payment_Adjust_End_Date,"-",""),8, "01")  as PAYMENTENDDT

from
`cityblock-analytics.abs_risk.tufts_mmr_monthly`

where
Payment_Date is not null
and Payment_Date <> ""

),


mmr as (

select *

from (
select * from tuftsMMR
union distinct
select * from tuftsMMR2) mmr

left join
masterMember mmt
on upper(substr(trim(mmr.First_Initial),0,1)) = upper(substr(trim( firstname),0,1 ))
and upper(substr(trim(mmr.Surname),0,6)) = upper(substr(trim( lastname ),0,6))
and mmr.date_of_birth  =  dateofbirth
and eligyear = extract(year from Payment_Date)
),


pat as (

select
patientID,
substr(PAYMENTSTARTDT,1,4) as paymentYear,
payer,
lineofbusiness

from
mmr
),


MY as  (
select
paymentYear,
patientID,
risk_adjustment_factor_a as myScore

from (
select
patientID
,substr(PAYMENTSTARTDT,1,4) as paymentYear
,PAYMENTSTARTDT
,PAYMENTENDDT
, Months_Adj_Part_A as Months_Adj_Part_A
, IFNULL(CAST(Risk_Adjustment_Factor_A as numeric),0) as risk_adjustment_factor_a
, IFNULL(CAST(Months_Adj_Part_A as numeric),0) as Number_of_Payment_Adj_Months_Pt_A
, DENSE_RANK() OVER(PARTITION BY patientID , substr(PAYMENTSTARTDT,1,4)	 ORDER BY PAYMENTSTARTDT DESC, PAYMENTENDDT desc, Beneficiary_Id, IFNULL(CAST(Risk_Adjustment_Factor_A as numeric),0) desc) AS Ranked

from
mmr

where
mmr_adjustment_reason_code = "26"

order by
MBI)

where
ranked =1),


FY as (

select
paymentYear,
patientID,
risk_adjustment_factor_a as fyScore

from (
select
patientID
,substr(PAYMENTSTARTDT,1,4) as paymentYear
,PAYMENTSTARTDT
,PAYMENTENDDT
,Months_Adj_Part_A as Months_Adj_Part_A
,IFNULL(CAST(Risk_Adjustment_Factor_A as numeric),0) as risk_adjustment_factor_a
,IFNULL(CAST(Months_Adj_Part_A as numeric),0) as Number_of_Payment_Adj_Months_Pt_A
,DENSE_RANK() OVER(PARTITION BY patientID , substr(PAYMENTSTARTDT,1,4)	 ORDER BY PAYMENTSTARTDT DESC, PAYMENTENDDT desc, Beneficiary_Id) AS Ranked

from
mmr

where
mmr_adjustment_reason_code = "25"

order by
patientID,
paymentYear,
Ranked
)

where
ranked =1
),

final as (

select distinct
pat.* ,
myScore,
fyScore

from
pat

left join
FY
on
pat.patientID = FY.patientID
and pat.paymentYear = FY.paymentYear

left join
MY
on
pat.patientID = MY.patientID
and pat.paymentYear = MY.paymentYear

where
myScore is not null
or fyScore is not null
or (myScore is not null and fyScore is not null)

order by
patientID,
paymentYear)

select * from final