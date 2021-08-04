with visit_note_doc as (

select distinct *
from
{{source('elation_mirror','visit_note_document_tag_latest')}}
),


visit_note as (

select distinct *

from
{{source('elation_mirror','visit_note_latest')}}
),


appointment_latest as (

select distinct *

from
{{source('elation_mirror','appointment_latest')}}
),

doc_tag as (

select distinct *
from
{{source('elation_mirror','document_tag_latest')}}
),


user_latest as (

select distinct *
from
{{source('elation_mirror','user_latest')}}
),


mm as (

select distinct *
from
{{ref('master_member_v1')}}

where
isCityblockmembermonth = true
and patientID is not null
),


xc as (

select distinct *

from
{{source('codesets','ICD10toHCCcrosswalk')}}

where
CMSHCCModelCategoryV24forCurrentYear = 'Yes'
),


final as (

select distinct
visit_note.id,
a.appt_type,
a.appt_time,
a.status,
a.patient_id as elationID,
mm.patientID,
partnerName,
lineofBusinessGrouped as lineofBusiness,
visit_note.name,
document_date,
first_name,
last_name,
visit_note.last_modified,
visit_note.creation_time,
visit_note.deletion_time,
signed_time,
trim(coalesce(CMSHCCModelCategoryV24or23, replace(trim(SPLIT(value, ' ')[OFFSET(0)]),'HCC',''))) as hcc,
doc_tag.description,
value,
trim(SPLIT(value, ' ')[OFFSET(0)]) as dxCode,
trim(SPLIT(value, ' ')[OFFSET(1)]) as Result,
dense_rank() over (partition by a.patient_id, trim(SPLIT(value, ' ')[OFFSET(0)]), document_date order by visit_note.last_modified desc) as ranked

from
appointment_latest a

left join
visit_note
on a.patient_id = visit_note.patient_id
and date(a.appt_time) = date(visit_note.document_date)
and date(a.appt_time) <= date(visit_note.signed_time)

left join
visit_note_doc
on visit_note.id = visit_note_doc.visit_note_id

left join
doc_tag
on visit_note_doc.document_tag_id = doc_tag.id

left join
user_latest
on visit_note.physician_user_id = user_latest.id

left join
mm
on cast(a.patient_id as string) = mm.elationID

left join
xc
on cast(extract(year from document_date) as string) = year
and DiagnosisCode = trim(SPLIT(value, ' ')[OFFSET(0)])

where
value is not null
and
(code like '%CB%'
or
(value like '%RESOLVED%'
or value like '%DISCONFIRMED%'
or value like '%INCONCLUSIVE%'
or value like '%CONFIRMED%')
)

order by
document_date
)

select * from final where ranked =1
