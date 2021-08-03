with mmr_file as (

select *,
substr(data.Payment_Start,1,4) as paymentYear

from
{{source('emblem_revenue','MMR_20*')}}
),


masterMember as (

select distinct
eligYear,
patientid,
firstname,
lastname,
gender,
dateofbirth,
medicareId,
memberID,
lineofbusiness

from
{{ref('hcc_risk_members')}}

where
lineofbusiness not in ( 'medicaid', 'commercial')
),


mmr as (

select mmr_file.*,
mmt.lineofbusiness,
'emblem' as payer

from
mmr_file

left join
masterMember mmt
on mmr_file.patient.patientid  = cast(mmt.patientId as string)
and mmr_file.paymentYear = cast(mmt.eligyear as string)
),


pat as (

select
patient.patientID,
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
,data.Run_date
,data.Plan as contract_number
,patient.patientID
,data.HIC_Number
,trim(data.HIC_Number) as MBI
,rpad(replace(data.Payment_Start,"-",""),8, "01")  as PAYMENTSTARTDT
,rpad(replace(data.Payment_End,"-",""),8, "01")  as PAYMENTENDDT
,case when data.Adj_Reason is null then null
        when cast(data.Adj_Reason as numeric)  < 10 then concat('0',cast(data.Adj_Reason as string))
        when cast(data.Adj_Reason as numeric)  > 10 then cast(data.Adj_Reason as string)
            end as mmr_adjustment_reason_code
,data.Months_Adj_Part_A as Months_Adj_Part_A
,IFNULL(CAST(data.Risk_Adj_Factor_A as FLOAT64),0) as risk_adjustment_factor_a
,cast(data.plan_benefit_package_id as INT64) as plan_benefit_package_id
,IFNULL(CAST(data.Number_of_Pymnt_Adj_Mnths_Pt_D as FLOAT64),0) as Number_of_Payment_Adj_Months_Pt_D
,IFNULL(CAST(data.Total_Part_D_Payment as FLOAT64),0) as Total_Part_D_Payment
,DENSE_RANK() OVER(PARTITION BY patient.patientID , substr(data.Payment_Start,1,4)	 ORDER BY data.Payment_Start DESC, data.Payment_End desc, data.HIC_Number) AS Ranked

from
mmr

where
data.Adj_Reason = "26"

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
,data.Run_date
,data.Plan as contract_number
,patient.patientID
,data.HIC_Number
,trim(data.HIC_Number) as MBI
,rpad(replace(data.Payment_Start,"-",""),8, "01")  as PAYMENTSTARTDT
,rpad(replace(data.Payment_End,"-",""),8, "01")  as PAYMENTENDDT
,case when data.Adj_Reason is null then null
        when cast(data.Adj_Reason as numeric)  < 10 then concat('0',cast(data.Adj_Reason as string))
        when cast(data.Adj_Reason as numeric)  > 10 then cast(data.Adj_Reason as string)
            end as mmr_adjustment_reason_code
, data.Months_Adj_Part_A as Months_Adj_Part_A
,IFNULL(CAST(data.Risk_Adj_Factor_A as FLOAT64),0) as risk_adjustment_factor_a
,IFNULL(CAST(data.Number_of_Pymnt_Adj_Mnths_Pt_D as FLOAT64),0) as Number_of_Payment_Adj_Months_Pt_D
,IFNULL(CAST(data.Total_Part_D_Payment as FLOAT64),0) as Total_Part_D_Payment
,DENSE_RANK() OVER(PARTITION BY patient.patientID , substr(data.Payment_Start,1,4)	 ORDER BY data.Payment_Start DESC,data.Payment_End desc,data.HIC_Number,IFNULL(CAST(data.Risk_Adj_Factor_A as FLOAT64),0)  desc) AS Ranked

from
mmr

where
data.Adj_Reason = "25"

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