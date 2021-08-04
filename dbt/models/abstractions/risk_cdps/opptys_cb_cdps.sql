
-------------------------------------
--- Section One: Reference Tables ---
-------------------------------------


--member, payer, and lob info
with member as (

select distinct
patientId,
memberId,
partner,
lineOfBusiness

from
{{ ref('hcc_risk_members') }}

where
eligYear = extract(Year from current_date())
and eligNow = true
and lineOfBusiness = 'medicaid'
and isHarp = false
),


cdps_cat_information as (

select *
from
{{source('cdps', 'cdps_cat_information')}}
),


--NPI Data
npidata as (

select distinct
Provider_First_Name as providerFirstName,
Provider_Last_Name_Legal_Name as providerLastNameLegalName,
cast(npi as string) as npi

from
{{ source('nppes','npidata_20210411')}}
),


--dx info all sources
abs_diagnoses as (

select distinct
patientID,
clmYear,
evidenceDate,
dxCode,
dxDescription,
providerName,
providerNPI,
category,
version

from
{{ ref('risk_claims_patients') }}

inner join
{{ source('cdps', 'cdps_maps') }}
on dxCode = dx

where
clmyear between extract(Year from current_date() ) -3 and extract(Year from current_date() )
),


yearCurrent as (

select distinct
patientID,
category,
version,
dxCode as dxCode,
evidenceDate,
dxdescription as dxdescription,
concat(dxCode," - ", ifnull(dxdescription,"discription missing"), " coded on ", evidenceDate, " by ", ifnull(providerName,"unknown provider")) as evidence1

from
(select *,
DENSE_RANK() OVER(PARTITION BY patientID, category ORDER BY dxCode DESC) AS DensePowerRank

from (
SELECT distinct
*,
DENSE_RANK() OVER(PARTITION BY yearspast.patientID, yearspast.dxCode ORDER BY evidenceDate DESC, providerName nulls last) AS DenseRank,

FROM
abs_diagnoses yearspast

where
clmYear = extract(year from current_date())
)

where
DenseRank =1
)

where
DensePowerRank =1
),


timesCoded as (

SELECT distinct
patientID,
category,
count(distinct concat(dxCode,evidenceDate) ) as clmCount

FROM
abs_diagnoses

where
clmYear between (extract(year from current_date()) - 3) and ( extract(year from current_date()) - 1)

group by
patientID,
category
),


yearspast as (

select distinct
patientID,
category,
version,
providerNPI,
evidenceDate,
dxCode as underlyingDxCode,
dxdescription as dxdescription,
concat(dxCode," - ", ifnull(dxdescription,"discription missing"), " coded on ", evidenceDate, " by ", ifnull(providerName,"unknown provider")) as evidence1

from
(select *,
DENSE_RANK() OVER(PARTITION BY patientID, category ORDER BY dxCode DESC) AS DensePowerRank

from (
SELECT distinct
*,
DENSE_RANK() OVER(PARTITION BY yearspast.patientID, yearspast.dxCode ORDER BY evidenceDate DESC, providerName nulls last) AS DenseRank,

FROM
abs_diagnoses yearspast

where
clmYear between (extract(year from current_date()) - 3) and ( extract(year from current_date()) - 1)
)

where
DenseRank =1
)
where
DensePowerRank =1
),


final as (

select distinct
mem.partner,
mem.patientId,
mem.lineOfBusiness,
'cityblock' as conditionsource,
'PERS' as conditionType,
'CDPS' as conditionCategory,
yearspast.category as conditionCategoryCode,
c.definition as conditionName,
case when yearCurrent.category is null then 'OPEN'
     else 'CLOSED'
         end as conditionStatus,
yearspast.underlyingDxCode,
case when d.clmCount >=50 then "VERY HIGH"
     when d.clmCount >= 4 and d.clmCount < 50 then "HIGH"
     when d.clmCount = 3 then "MEDIUM"
     when d.clmCount = 2 then "LOW"
     when d.clmCount = 1 then "VERY LOW"
	 else "MEDIUM"
		 end as confidenceLevel,
case when d.clmCount >=50 then 1
     when d.clmCount >= 4 and d.clmCount < 50 then 2
     when d.clmCount = 3 then 3
     when d.clmCount = 2 then 4
     when d.clmCount = 1 then 5
	 else 3
		  end as confidenceNumeric,
yearspast.underlyingDxCode as supportingEvidenceCode,
"ICD" as supportingEvidenceCodeType,
'' as supportingEvidenceValue,
concat( ifnull(yearspast.underlyingDxCode,''), " - ",ifnull(yearspast.dxDescription,'Diagnosis Code'))  as supportingEvidenceName,
'claims' as supportingEvidenceSource,
current_date() as dateUploaded,
providerFirstName,
providerLastNameLegalName,
d.clmCount,
coalesce( yearCurrent.evidenceDate, yearspast.evidenceDate ) as clmLatest,
"1" rownum1,
case when yearCurrent.category is null then 'no claim match'
     else 'claim match'
          end as clmFlg

from
yearspast

inner join
member mem
on mem.patientId = yearspast.patientId

left join
yearCurrent
on yearspast.category = yearCurrent.category
and yearspast.patientID = yearCurrent.patientID

left join
timesCoded d
on yearspast.category = d.category
and yearspast.patientID = d.patientID

left join
cdps_cat_information c
on yearspast.category = c.category

left join
npidata
on providerNPI = npi

)


select * from final
