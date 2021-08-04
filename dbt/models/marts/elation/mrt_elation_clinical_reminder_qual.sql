
{{ config(
tags = ["evening"]
)
}}


with qm_rows as (

SELECT
memberID,
measureID,
statusID,
ROW_NUMBER() OVER( PARTITION BY memberID, measureID order by setON desc) as rownum

from
{{source('quality_measure_mirror','member_measure')}} qm
)
,

max as (

SELECT distinct
memberID,
code,
m.name as measureName,
displayName,
mms.name as measureStatus

from
qm_rows qm

LEFT JOIN
{{source('quality_measure_mirror','measure')}} m
on m.id = qm.measureID

LEFT JOIN
{{source('quality_measure_mirror','member_measure_status')}} mms
on mms.id = qm.statusID

where
rownum = 1
),


pic as (

select
mem.id,
mem.mrnId as memberid,
mrn.mrn as externalID,

from
{{source('member_index', 'member')}} mem

left join
{{source('member_index','mrn')}} mrn
on mem.mrnId = mrn.id
),


final as (

SELECT
cast(format_date("%m/%d/%Y",current_date()) as string) as Date,
pic.externalID as Patient_ID,
"Elation" as Patient_System,
upper(p.firstName) as First_Name,
upper(p.lastName) as Last_Name,
cast(format_date("%m/%d/%Y",m.dateOfBirth) as string)  as Date_of_Birth,
CASE WHEN m.gender = 'male' THEN 'M' WHEN m.gender = 'female' THEN 'F' else "U" END as Gender,
concat(REPLACE(REPLACE(REPLACE(REPLACE(code, 'HEDIS ','CB_'),'ABLE ', 'CB_'), 'AWV', 'AWE'),'FV','FI'), "_2021") as Measure_ID,
UPPER(measureStatus)  AS Measure_Status,
case when patientHomeMarketName = 'Massachusetts' then '1538441167'
     when patientHomeMarketName = 'Connecticut' then '1487713947' end as Provider_Assigned_NPI,
case when patientHomeMarketName = 'Massachusetts' then 'Taylor, James'
     when patientHomeMarketName = 'Connecticut' then 'Lennon, Stephanie' end as Provider_Assigned,
case when patientHomeMarketName = 'Massachusetts' then 'Cityblock - Massachusetts'
     when patientHomeMarketName = 'Connecticut' then 'Cityblock - Connecticut' end as Practice_Name

FROM

max h

left JOIN
pic
on
h.memberid = pic.id

LEFT JOIN
{{source('commons','patient')}} p
ON p.id = h.memberID

LEFT JOIN
{{ref('member_info')}} m
on m.patientid = h.memberID

LEFT JOIN
{{ref('member')}} mm
ON mm.patientID = h.memberID

WHERE
measureStatus IN ("open", "closed")
AND patientHomeMarketName in ( "Connecticut", "Massachusetts")

GROUP BY
Date,
Patient_ID,
Patient_System,
First_Name,
Last_Name,
Date_of_Birth,
Gender,
Measure_ID,
Measure_Status,
Provider_Assigned_NPI,
Provider_Assigned,
Practice_Name
)


SELECT * from final
