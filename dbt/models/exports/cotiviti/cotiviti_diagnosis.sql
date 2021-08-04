with claims_self_service_ordered as (

  select *
  from {{ ref('mrt_claims_self_service') }}
  where claimLineStatus = 'Paid'
        and costClassification1 != 'RX'
  order by patientId,
           claimId,
           lineNumber,
           formType
),

claim_lines as (

	select 
    patientId,
	  claimId,
	  lineNumber,
	  min(dateFrom) as dateFrom,
	  max(dateTo) as dateTo,
	  max(coalesce(amountAllowed, amountPlanPaid)) as amountAllowed,
	  string_agg(distinct placeOfServiceDescription) as placeOfServiceDescription,
    string_agg(distinct outpatientLocation) as outpatientLocation,
	  string_agg(distinct billType) as billType,
	  string_agg(distinct principalProcedureCode) as principalProcedureCode,
	  string_agg(distinct principalDiagnosisCode) as principalDiagnosisCode,
	  string_agg(distinct procedureCode) as procedureCode,
	  string_agg(distinct revenueCode) as revenueCode,
	  string_agg(distinct claimLineStatus) as claimLineStatus,
	  string_agg(distinct claimCategory) as claimCategory,
	  max(cast(surgical as bool)) as surgical,
	  string_agg(distinct costClassification1) as costClassification1,
	  string_agg(distinct costClassification2) as costClassification2,
	  string_agg(distinct finalClassification) as finalClassification,
	  string_agg(distinct lineOfBusiness) as lineOfBusiness
  
	from claims_self_service_ordered
	group by 1, 2, 3
),

gold_claims_facility as (

  select claimId,
         header.date.paid as datePaid,
         -- Sort diagnosis codes array such that 'principal' comes before all 'secondary' diagnosis codes
         -- Secondly, sort the rest of diagnosis codes alphabetically
         array(select x from unnest(header.diagnoses) AS x ORDER BY x.tier in ('principal', 'principle') desc, x.code) as diagnoses

  from {{ ref('facility_gold_all_partners') }}
),

gold_claims_professional as (

  select claimId,
         lines,
         -- Sort diagnosis codes array such that 'principal' comes before all 'secondary' diagnosis codes
         -- Secondly, sort the rest of diagnosis codes alphabetically
         array(select x from unnest(header.diagnoses) AS x ORDER BY x.tier in ('principal', 'principle') desc, x.code) as diagnoses
  from {{ ref('professional_gold_all_partners') }}
),

diagnosis_codes_unioned as (

  select claimId, diagnoses from gold_claims_facility
  
    union all
  
  select claimId, diagnoses from gold_claims_professional
),

diagnosis_codes as (

  select distinct
    claimId,

    {% for i in range(25) %}
      diagnoses[safe_offset( {{ i }} )].code as diagnosisCode{{ i + 1 }},
    {% endfor %}

    diagnoses[safe_offset(0)].codeset as principalCodeset
    
  from diagnosis_codes_unioned
),

paid_dates_facility as (

  select
    claimId,
    max(datePaid) as datePaid
  from gold_claims_facility
  group by 1
),

paid_dates_professional as (

  select
    claimId,
    line.lineNumber,
    max(line.date.paid) as datePaid
  from gold_claims_professional
  cross join unnest(gold_claims_professional.lines) as line
  group by 1, 2
),

source_service_location_algo as (

  select
    patientId,
    claimId,
    lineNumber,
    dateFrom,
    dateTo,
	  amountAllowed,

    -- Classify the source of the claim
    case
      when regexp_contains(costClassification1, 'IP') and regexp_contains(costClassification2, 'Facility') then 'Inpatient Facility'

      when costClassification1 in ('Transport', 'RX', 'DME') then 'Diagnostics, DME, Other'
      -- Find diagnostic revenue codes
      when regexp_contains(revenueCode, '(032[0-9]|0341|0343|035[0-9]|040[0-9]|047[0-9]|092[0-9])') then 'Diagnostics, DME, Other'
      -- Diagnostic radiology procedure codes
      when procedureCode between '70010' and '76499' then 'Diagnostics, DME, Other'
      when procedureCode between '76506' and '76999' then 'Diagnostics, DME, Other'
      when procedureCode between '77046' and '77067' then 'Diagnostics, DME, Other'
      when procedureCode between '77071' and '77086' then 'Diagnostics, DME, Other'
      -- Diagnostic medicine procedure codes
      when procedureCode between '93880' and '93998' then 'Diagnostics, DME, Other'
      when procedureCode between '96105' and '96146' then 'Diagnostics, DME, Other'
      -- Diagnostic screening procedure codes
      when procedureCode in ('G0202', 'G0206', 'G0204', 'G0279', 'G0297') then 'Diagnostics, DME, Other'
      -- Injections for diagnostics procedure codes
      when procedureCode in ('A9500', 'A9502', 'A9503', 'A9541', 'A9552', 'A9575', 'A9579', 'A9585', 'Q9963', 'Q9966', 'Q9967') then 'Diagnostics, DME, Other'
      -- HCPCS level II code starting with 'E' or 'K' is for DME
      when procedureCode like 'E%' or procedureCode like 'K%' then 'Diagnostics, DME, Other'
      -- HCPCS level II code starting with 'R' is for diagnostic radiology
      when procedureCode like 'R%' then 'Diagnostics, DME, Other'

      when costClassification1 in ('ED', 'BH', 'Dialysis', 'Home Health', 'Observation', 'Other', 'Outpatient',
      	                           'OV Other', 'Primary Care', 'Specialty Care', 'Infusion') then 'Outpatient / Clinicians'
      when costClassification1 like 'OP %' then 'Outpatient / Clinicians'
      when costClassification1 in ('Hospice', 'SNF') and costClassification2 in ('Outpatient', 'Professional') then 'Outpatient / Clinicians'
      when regexp_contains(costClassification1, 'IP') and regexp_contains(costClassification2, 'Prof') then 'Outpatient / Clinicians'
      -- Find therapeutic revenue codes
      when regexp_contains(revenueCode, '(026[0-9]|027[0-9]|033[0-9]|0342|0344)') then 'Outpatient / Clinicians'
      -- Therapeutic radiology procedure codes
      when procedureCode between '77001' and '77022' then 'Outpatient / Clinicians'
      when procedureCode between '77261' and '77799' then 'Outpatient / Clinicians'
      when procedureCode between '78012' and '79999' then 'Outpatient / Clinicians'
      when procedureCode in ('G6001', 'G6002', 'G6004', 'G6012', 'G6013', 'G6014', 'G6015', 'G6017') then 'Outpatient / Clinicians'
      -- Medical care
      when procedureCode in ('A9581') then 'Outpatient / Clinicians'
      -- Surgery procedure codes
      when procedureCode between '10021' and '69990' then 'Outpatient / Clinicians'
      -- Assume all lab work not explicitly labeled as diagnostic is in support of a therapy
      when costClassification1 = 'Labs' then 'Outpatient / Clinicians'

      when costClassification1 in ('Hospice', 'SNF') and regexp_contains(costClassification2, 'Facility') then 'Long Term Care, Other Facilities'
    end as sourceDescription,

    -- Classify the location at which the service was delivered
    case
      when placeOfServiceDescription = 'Emergency Room - Hospital' then 'Emergency Department'
      when placeOfServiceDescription is null and costClassification1 = 'ED' then 'Emergency Department'

      when placeOfServiceDescription like '%Inpatient%' then 'Inpatient Admission'
      when placeOfServiceDescription is null and regexp_contains(costClassification1, 'IP') then 'Inpatient Admission'

      when placeOfServiceDescription in ('Adult Living Care Facility', 'Assisted Living Facility', 'Custodial Care Facility', 'Hospice',
      	                                 'Intermediate Care Facility/Mentally Retarded', 'Nursing Facility', 'Skilled Nursing Facility') then 'Long Term Care'

      when placeOfServiceDescription is null and costClassification1 in ('Hospice', 'SNF') then 'Long Term Care'
      when outpatientLocation in ('nonhospital_hospice', 'hospital_hospice', 'snf') then 'Long Term Care'

      when placeOfServiceDescription is not null and outpatientLocation is not null then 'Other'
    end as serviceLocationDescription,

    case when surgical = true then 1 else 0 end as surgicalFlag

  from claim_lines
),

final as (

  select
    algo.patientId,
    algo.claimId,
    algo.lineNumber,
    algo.dateFrom,
    algo.dateTo,
    coalesce(pdf.datePaid, pdp.datePaid) as datePaid,
    algo.amountAllowed,

    {% for i in range(25) %}
      dc.diagnosisCode{{ i + 1 }},
    {% endfor %}

    dc.principalCodeset,
    case
      when dc.principalCodeset = 'ICD9Cm' then 9
      when dc.principalCodeset = 'ICD10Cm' then 10
    end as icdVersion,


    algo.sourceDescription,
    case
      when sourceDescription = 'Inpatient Facility' then 2
      when sourceDescription = 'Outpatient / Clinicians' then 4
      when sourceDescription = 'Long Term Care, Other Facilities' then 6
      when sourceDescription = 'Diagnostics, DME, Other' then 8
      else 8
    end as source,

    algo.serviceLocationDescription,
    case
      when serviceLocationDescription = 'Emergency Department' then 'E'
      when serviceLocationDescription = 'Inpatient Admission' then 'I'
      when serviceLocationDescription = 'Long Term Care' then 'L'
      else 'O'
    end as serviceLocation,

    algo.surgicalFlag

  from source_service_location_algo algo
  left join diagnosis_codes dc using (claimId)
  left join paid_dates_facility pdf using (claimId)
  left join paid_dates_professional pdp using (claimId, lineNumber)
)

select *
from final
