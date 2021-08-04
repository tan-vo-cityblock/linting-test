
-------------------------------------
--- Section One: Reference Tables ---
-------------------------------------


--load member list
with lab_results as (

select distinct *

from
{{ ref('lab_results') }}

where
memberIdentifier is not null
and memberIdentifierField ='patientId'
and EXTRACT(YEAR FROM date) between EXTRACT(YEAR FROM current_date()) - 2 and EXTRACT(YEAR FROM current_date()) 
),


-------------------------------------
--- Section Two: Lab Claims ----
-------------------------------------


--aggregate lab data for patients
lab_claims as (

select distinct 
concat(memberIdentifier,name,loinc,date) as labId, 
date as evidenceDate,
memberIdentifier as patientId,  
name,
resultNumeric, 
units,
cast(REGEXP_REPLACE(loinc,'[^0-9 ]','') as string)  AS loinc

from
lab_results
)

select * from lab_claims

