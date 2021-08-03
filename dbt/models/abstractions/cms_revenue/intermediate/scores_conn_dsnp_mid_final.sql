with hcc_risk_members as (

select distinct
eligYear,
patientid,
firstname,
lastname,
dateofbirth,
medicareId,
memberID,
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

select *
from
{{source('Settlement_CCI','NMI_MBI_Crosswalk')}}
),


mmr as (

select distinct
eligyear,
payer,
mmr.lineofbusiness,
Run_Date,
xw.PATIENTID AS PATIENTID,
firstname,
lastname,
dateofbirth,
Payment_Date,
trim(Beneficiary_Id) as Beneficiary_Id ,
mmr.gender,
risk_adjustment_factor_a,
MMR_Adjustment_Reason_Code,
cast(extract(year from Payment_Adjustment_Start_Date) as string)as paymentYear,
Number_of_Payment_Adjustment_Months_Part_A as Months_Adj_Part_A,
rpad(replace(cast(Payment_Adjustment_Start_Date as string),"-",""),8, "01")  as PAYMENTSTARTDT,
rpad(replace(cast(Payment_Adjustment_End_Date as string),"-",""),8, "01")  as PAYMENTENDDT

from
connMMR mmr

inner join
NMI_MBI_Crosswalk xw
ON trim(mmr.Beneficiary_Id) = trim(xw.MBI)

left join
hcc_risk_members mmt
on upper(trim(xw.patientID)) = upper(trim(mmt.patientID ))
and extract(year from Payment_Date ) = eligyear
and mmr.lineofbusiness = mmt.lineofbusiness
),


pat as (

select
patientID,
paymentYear,
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
paymentYear
,Run_date
,patientID
,Beneficiary_Id
,trim(Beneficiary_Id) as MBI
,PAYMENTSTARTDT
,PAYMENTENDDT
,Months_Adj_Part_A
,risk_adjustment_factor_a
,DENSE_RANK() OVER(PARTITION BY patientID , substr(PAYMENTSTARTDT,1,4)	 ORDER BY PAYMENTSTARTDT DESC,PAYMENTENDDT desc,Beneficiary_Id) AS Ranked

from
mmr

where
MMR_Adjustment_Reason_Code = "26"

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
paymentYear
,Run_date
,patientID
,Beneficiary_Id
,trim(Beneficiary_Id) as MBI
,PAYMENTSTARTDT
,PAYMENTENDDT
,Months_Adj_Part_A
,risk_adjustment_factor_a
,DENSE_RANK() OVER(PARTITION BY patientID , substr(PAYMENTSTARTDT,1,4)	 ORDER BY PAYMENTSTARTDT DESC,PAYMENTENDDT desc,Beneficiary_Id,risk_adjustment_factor_a desc) AS Ranked

from
mmr

where
MMR_Adjustment_Reason_Code = "25"

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