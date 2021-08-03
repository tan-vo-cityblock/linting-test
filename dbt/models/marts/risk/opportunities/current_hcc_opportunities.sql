
{{
  config(
       tags=["weekly"]
  )
}}



with current_member as (
select distinct
* 
from
{{ ref('hcc_risk_members') }} hcc_risk_members
    where 
     eligNow = true
    and eligYear = extract(Year from current_date())

and   
(lower(lineOfBusiness) like '%medicare%' 
or lower(lineOfBusiness) like '%dsnp%' 
or lower(lineOfBusiness) like '%dual%'
or lower(lineOfBusiness) like '%medicaid%')
),


all_suspects_to_providers as (
select distinct
* 
from 
{{ ref('all_suspects_to_providers_int') }} dx

where
(lower(dx.lineOfBusiness) like '%medicare%' 
or lower(dx.lineOfBusiness) like '%dsnp%' 
or lower(dx.lineOfBusiness) like '%dual%'
or lower(dx.lineOfBusiness) like '%medicaid%')
),


all_suspects_ranked as (
select distinct
* 
from
{{ ref('all_suspects_ranked') }} dx

where
(lower(dx.lineOfBusiness) like '%medicare%' 
or lower(dx.lineOfBusiness) like '%dsnp%' 
or lower(dx.lineOfBusiness) like '%dual%'
or lower(dx.lineOfBusiness) like '%medicaid%')
and conditionCategoryCode is not null
and conditionstatus = 'OPEN'
),


hx_claims as (
select distinct *
FROM
{{ ref('risk_claims_patients') }}

where excludeICD <> true 
and excludeAcuteHCC <> true 
and extract(year from evidenceDate)   between extract(year from current_date() ) - 3 and extract(year from current_date()) -1
),


abs_diagnoses as (
select distinct 
dx.* except( diagnosisTier,  claimLineStatuses,  placesOfService) 
from 
{{ ref('abs_diagnoses') }} dx

where 
memberIdentifierField = 'patientId'
and extract(year from serviceDateFrom) = extract (year from (current_date()))
),


cci_icd10cm as (
select distinct
* 
from 
{{ source('codesets', 'cci_icd10cm') }}
),


hcc_rollup_2020 as (
select distinct 
* 
from
{{ source('codesets', 'hcc_rollup_2020') }} 
),


actuary_condition_codes as (
select distinct
lineOfBusiness as lob,  
code, coefficient,  
value , 
codeType
from
{{ source('codesets', 'actuary_condition_codes') }}
),



provider as (
select distinct patientid pcppatid,
claimsAttributedPcpNpi,
claimsAttributedPcpName
from 
{{ ref('member') }}
),


apptWithPCP as (
select distinct memberIdentifier as PatientPcp, 
max (case when serviceDateFrom is not null then true end ) as hadAppointment,
max(case when claimsAttributedPcpName is not null or claimsAttributedPcpNPI is not null then true end )as sawAttribPpcOrSpec
from
 (select distinct 
 * 
 from
 abs_diagnoses
  where
  cast(EXTRACT(YEAR FROM  serviceDateFrom ) as string)  = '2020'
  and
      ((lower(sourceType) like '%claim%'
      and source in('facility','professional')
      )
      or
      (lower(source) like '%ccd%'
      and partnerName in ('acpny', 'elation') ))) a
      left join
      provider b
      on
      (a.memberidentifier = b.pcppatid
      and a.providerNpi = b.claimsAttributedPcpNpi)
      or
      (a.memberidentifier = b.pcppatid
      and upper(a.providerName) = upper(b.claimsAttributedPcpName))
      group by 
      memberIdentifier
),


hospitalizedct as (
select distinct 
memberIdentifier as PatientHosp, 
count(distinct concat(serviceDateTo,serviceDateFrom) ) as visits
from
(select distinct 
memberIdentifier, servicedateto, serviceDateFrom 
from
abs_diagnoses
where
encounterType	= 'inpatient'
            and cast(EXTRACT(YEAR FROM  serviceDateFrom ) as string)  = '2020'
            and date_diff(	serviceDateTo,serviceDateFrom, day) >1
            and
      ((lower(sourceType) like '%claim%'
      and source in('facility')
      )
    )) a
      group by 
      memberIdentifier
),
    

cbappt as (
select distinct 
memberIdentifier as cbapptmemb,
providername,
serviceDateFrom 
from
    (select distinct 
    memberIdentifier, 
    cityblockProvider,
    providername,
    serviceDateFrom ,
    DENSE_RANK() OVER (PARTITION BY memberIdentifier ORDER BY  serviceDateFrom desc, providername) AS drank
    from
    abs_diagnoses
    
    where
    EXTRACT(YEAR FROM  serviceDateFrom ) =  EXTRACT(YEAR FROM  current_date() )
    and
    (lower(source) like '%ccd%'
    and partnerName in ('acpny', 'elation')
    and cityblockProvider ='Yes'
    and encounterType in (
    'home care visit',
    'house call visit note',
    'office visit',
    'office visit note',
    'telehealth',
    'procedure visit',
    'telemedicine',
    'telemedicine note',
    'visit note'))
    )
     where drank = 1
     order by memberIdentifier
),   


diag_history as (
select distinct 
*
FROM
{{ ref('risk_claims_patients') }}

where 
EXTRACT(YEAR FROM evidenceDate)  =  EXTRACT(YEAR FROM  current_date() )
),


firstcoded as (
select distinct 
patientID, 
memhcccombo as capCombo, 
capturedHCC, 
lineOfBusiness, 
partner,
HCCDescription,
firstCodedDate, 
extract(month from firstCodedDate) as firstcodedMonth
from
	(select distinct 
    clmYear, 
    dxs.patientID, 
    hcc as capturedHCC,
    memhcccombo,
    HCCDescription,
    min(evidenceDate) as firstCodedDate,
    lineOfBusiness,
    partner
	from diag_history dxs

	group by
	clmYear, 
  dxs.patientID, 
  hcc, 
  memhcccombo,
  HCCDescription,
  lineOfBusiness, 
  partner
  )
)
,



everoutstanding as ( 
select distinct 
demo.partner, 
dx.* ,
code,	
hccs.coefficient, 
concat(dx.patientId, conditionCategoryCode) as suspcombo
from 
current_member demo
inner join
      (
          select distinct
          *
          from
          (select
            distinct
            patientId,
            lineofbusiness,
            case when ConditionName = 'Diabetes without Complication' then 'Diabetes without Complications'
                   when ConditionName = 'Chronic Kidney Disease' then 'Chronic Kidney Disease, Moderate (Stage 3)'
                   else ConditionName
              end as ConditionName,
            conditionCategoryCode,
            min(conditiontype) as conditiontype,
            concat(patientID,conditionCategoryCode) as patHCCCombo
            from
            all_suspects_ranked
            group by
            patientId,
            lineOfBusiness,
            conditionName,
            conditionCategoryCode,
            concat(patientID,conditionCategoryCode) 
            order by
            patientId,conditionName,patHCCCombo)
           
        union distinct
         
          (select
            distinct
            patientId ,
            lineofbusiness,
            case when HCCDescription = 'Diabetes without Complication' then 'Diabetes without Complications'
                   when HCCDescription = 'Chronic Kidney Disease' then 'Chronic Kidney Disease, Moderate (Stage 3)'
                   else HCCDescription
            end as ConditionName,
            hcc as conditionCategoryCode,
            'PERS' as conditiontype,
            memhcccombo as patHCCCombo
            from
            hx_claims
            )
            
      ) dx
on
demo.patientid = dx.patientID 
and lower(demo.lineOfBusiness) = lower( dx.lineofbusiness )

inner join
actuary_condition_codes hccs
on dx.conditionCategoryCode = hccs.code 
and lower(dx.lineOfBusiness) = lower( hccs.lob )
where
 dx.lineOfBusiness <> 'commericial'
)
,


captured as (
select distinct * from
(
select distinct
rolled.partner, rolled.lineofbusiness,
rolled.patientId,
capturedHCC, 
HCCDescription,
coefficient as capturedCoefficient , 
'CAPTURED' as component,
concat(rolled.patientId, capturedHCC) as capCombo, 
case when conditionType is null then 'NEW' else conditionType end as conditionType
,dense_rank() over (partition by patientId, capturedHCC order by case when conditiontype ='PERS' then 1 when conditionType = 'SUSP' then 2 when conditionType = 'NEW' then 3 end) ranked
from
    (
    select  distinct
    a.*, b.hcc_rollup, 
    dense_rank() over (partition by a.patientId, hcc_rollup order by patientId, hcc_rollup, cast(capturedHCC as numeric) nulls first) dr2
    from
    firstcoded a

    left join
    hcc_rollup_2020 b
    on a.capturedHCC =b.hcc
    ) rolled

left join
    (select distinct 
    pathcccombo,  
    conditionType 
    from everoutstanding) everoutstanding
    on concat(rolled.patientId, capturedHCC) = patHCCCombo

inner join
actuary_condition_codes hccs
on rolled.capturedHCC = hccs.code 
and lower(rolled.lineOfBusiness) = lower( hccs.lob )

where 
dr2 = 1 or hcc_rollup is null
) 

where ranked = 1
)
,


currentoutstanding as
(select * from
(select distinct
dx.partner , 
dx.lineOfBusiness, 
dense_RANK() OVER ( PARTITION BY dx.patientId, dx.partner ORDER BY dx.lineOfBusiness desc) AS rank,
conditionName,
dx.patientId, coefficient, value as dollarValue,
 conditionCategoryCode,
'OUTSTANDING' as component ,
conditionType,
underlyingDxCode, 
recaptureEvidence,
pharmEvidence,  
labEvidence,
claimsEvidence,
otherEvidence,
riskFactors,  
runDate
from
    (select 
    partner , 
    lineOfBusiness, 
    conditionName,
    patientId,
    case 
    when lineOfBusiness = 'medicaid' and conditionType = 'SUSP' and conditionCategoryCode = '19' then  '424'
    when lineOfBusiness = 'medicaid' and conditionType = 'SUSP' and conditionCategoryCode = '18' then  '428'
    else conditionCategoryCode end as conditionCategoryCode,
    'OUTSTANDING' as component ,
    conditionType,
    underlyingDxCode, 
    recaptureEvidence,
    pharmEvidence,  
    labEvidence,
    claimsEvidence,
    otherEvidence,
    riskFactors,  
    runDate 
    from
    all_suspects_to_providers) dx

inner join
actuary_condition_codes hccs
on dx.conditionCategoryCode = hccs.code 
and lower(dx.lineOfBusiness) = lower( hccs.lob )

where
concat(dx.patientID,conditionCategoryCode) not in (select distinct capcombo from captured)
)
where rank =1
)
,


-- Values will change with new year
-- list of opportunities for recapture

hccsrankedforrecap as (
select distinct partner, patientID, lineofbusiness, 
firstname, lastname, dateofbirth, cohortname, cohortgolivedate, currentstate, virtualprogram,
hccs.* ,
 case when conditionType = 'PERS' and partner ='connecticare'
          then cast(((finalCoeffValue / 1.069) * (1-0.0509) ) as numeric)*.63
     when conditionType = 'PERS' and partner in ('tufts','emblem')
          then cast(((finalCoeffValue / 1.069) * (1-0.0509) ) as numeric)*.65
     when conditionType = 'SUSP' and partner in ('tufts','emblem','connecticare')
          then(cast(((finalCoeffValue / 1.069) * (1-0.0509) ) as numeric)*.20) end          
          as finaloutstandingCoeffdeduct,
          
 case when conditionType = 'PERS' and partner ='connecticare'
          then cast(((finalCoeffValue / 1.069) * (1-0.0509) ) as numeric)
     when conditionType = 'PERS' and partner in ('tufts','emblem')
          then cast(((finalCoeffValue / 1.069) * (1-0.0509) ) as numeric)
     when conditionType = 'SUSP' and partner in ('tufts','emblem','connecticare')
          then(cast(((finalCoeffValue / 1.069) * (1-0.0509) ) as numeric)*.20) end          
          as finaloutstandingCoefffull
        
from
current_member mem
inner join
(
select distinct outstanding.patientId as outid, outline,	outstanding.conditionType,	outstanding.conditionName,	
captured.hcc1 as caphcc,  captured.coefficient as capcoeff, 
outstanding.hcc1 as outhcc, outstanding.coefficient as outcoeff,
case when cast(captured.hcc1 as numeric) < cast(outstanding.hcc1 as numeric) then 0
     when cast(captured.hcc1 as numeric) > cast(outstanding.hcc1 as numeric) then  round( outstanding.coefficient- captured.coefficient ,5)
     when cast(captured.hcc1 as numeric) is null then cast(outstanding.coefficient as numeric) 
     end as finalCoeffValue,
component ,
dollarValue,
underlyingDxCode, 
recaptureEvidence,
pharmEvidence,  
labEvidence,
claimsEvidence,
otherEvidence,
riskFactors
from
(select distinct *, 
case when hcc_rollup is null then hcc1 else hcc_rollup end as hcc_rollup1
from
(
select distinct *,
dense_rank() over (partition by patientId, hcc_rollup  order by patientId, hcc_rollup, cast(a.hcc1 as numeric) nulls first) dr2
from
(select distinct  
patientId, 
lineofbusiness as outline,
conditionType,
ConditionName, 
conditionCategoryCode as hcc1,
coefficient, 
dollarValue, 
component ,
underlyingDxCode, 
recaptureEvidence,
pharmEvidence,  
labEvidence,
claimsEvidence,
otherEvidence,
riskFactors
from currentoutstanding
) a
left join
hcc_rollup_2020 b
on a.hcc1 =b.hcc
) 
where dr2 =1 or dr2 is null
) outstanding   
left join
(select *, 
case when hcc_rollup is null then hcc1 else hcc_rollup end as hcc_rollup1 from
(
select distinct *,
dense_rank() over (partition by patientId, hcc_rollup  order by patientId, hcc_rollup, cast(a.hcc1 as numeric) nulls first) dr2
from
(select distinct partner, patientId, conditionType, HCCDescription, capturedHCC as hcc1, 
capturedCoefficient as coefficient from captured
) a
left join
hcc_rollup_2020 b
on a.hcc1 =b.hcc) 
where dr2 =1 or dr2 is null
) captured 
on outstanding.patientId = captured.patientId
and outstanding.hcc_rollup = captured.hcc_rollup1
) hccs
on mem.patientID = hccs.outid
and mem.lineofbusiness = hccs.outline
)
,


results as (
select distinct * except (rank)
from
(
select distinct
dx.partner,
currentState,
virtualProgram,
dx.lineOfBusiness, 
hadAppointment, sawAttribPpcOrSpec, visits as inpatientVisits, serviceDateFrom as cbservicedate,
dense_RANK() OVER ( PARTITION BY dx.patientId ORDER BY lastName , dateOfBirth) AS rank,
dx.patientId,
lastName, 
firstName,  
dateOfBirth,
cohortName, 
cohortGoLiveDate,
conditionType,
conditionName,
outhcc as conditionCategoryCode, 
underlyingDxCode, 
icd_10_cm_code_description as underlyingDxCodeDescr,
recaptureEvidence,
pharmEvidence,  
labEvidence,
claimsEvidence,
riskFactors,        
finaloutstandingCoefffull,
case when lower(dx.lineOfBusiness) like '%medicaid%' then cast (dollarValue as numeric) *12
     when dx.lineOfBusiness in ('dsnp','duals','medicare') and conditionType = 'SUSP' and dx.partner like '%conn%' then cast(finaloutstandingCoefffull as numeric)*12734.64
     when dx.lineOfBusiness in ('dsnp','duals','medicare') and conditionType = 'PERS' and dx.partner like '%conn%' then cast(finaloutstandingCoefffull as numeric)*12734.64

     when dx.lineOfBusiness in ('dsnp','duals','medicare') and conditionType = 'SUSP' and dx.partner like '%emblem%' then cast(finaloutstandingCoefffull as numeric)*15150.72
     when dx.lineOfBusiness in ('dsnp','duals','medicare') and conditionType = 'PERS' and dx.partner like '%emblem%' then cast(finaloutstandingCoefffull as numeric)*15150.72 

     when dx.lineOfBusiness in ('dsnp','duals','medicare') and conditionType = 'SUSP' and dx.partner like '%tufts%' then cast(finaloutstandingCoefffull as numeric)*16500.00
     when dx.lineOfBusiness in ('dsnp','duals','medicare') and conditionType = 'PERS' and dx.partner like '%tufts%' then cast(finaloutstandingCoefffull as numeric)*16500.00
	 end as dollarValueFull,

finaloutstandingCoeffdeduct,
case when lower(dx.lineOfBusiness) like '%medicaid%' then cast (dollarValue as numeric)*12
     when dx.lineOfBusiness in ('dsnp','duals','medicare') and conditionType = 'SUSP' and dx.partner like '%conn%' then cast(finaloutstandingCoeffdeduct as numeric)*12734.64
     when dx.lineOfBusiness in ('dsnp','duals','medicare') and conditionType = 'PERS' and dx.partner like '%conn%' then cast(finaloutstandingCoeffdeduct as numeric)*12734.64

     when dx.lineOfBusiness in ('dsnp','duals','medicare') and conditionType = 'SUSP' and dx.partner like '%emblem%' then cast(finaloutstandingCoeffdeduct as numeric)*15150.72
     when dx.lineOfBusiness in ('dsnp','duals','medicare') and conditionType = 'PERS' and dx.partner like '%emblem%' then cast(finaloutstandingCoeffdeduct as numeric)*15150.72 

     when dx.lineOfBusiness in ('dsnp','duals','medicare') and conditionType = 'SUSP' and dx.partner like '%tufts%' then cast(finaloutstandingCoeffdeduct as numeric)*16500.00
     when dx.lineOfBusiness in ('dsnp','duals','medicare') and conditionType = 'PERS' and dx.partner like '%tufts%' then cast(finaloutstandingCoeffdeduct as numeric)*16500.00
	 end as dollarValueDeduct
from
hccsrankedforrecap dx
left join
cci_icd10cm cd
on dx.underlyingDxCode = cd.icd_10_cm_code
left join
apptwithpcp pcp
on dx.patientID = pcp.PatientPcp
left join
hospitalizedct hosp8
on dx.patientID= hosp8.PatientHosp
left join
cbappt
on dx.patientID = cbappt.cbapptmemb
)
where rank =1
and finaloutstandingCoeffdeduct > 0
)

select distinct
*
from
results
