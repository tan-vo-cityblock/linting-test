
with gold_provider as (

    select * from {{ ref('provider_gold_all_partners') }}

),

member_demographics as (
  
    select
      memberId as patientId,
      dateOfBirth,
      dateOfDemise

    from {{ ref('src_member_demographics') }}

),

medical_union as (

    select * from {{ ref('abs_medical_union') }}

),

lob_emblem as (

    select * from {{ ref('abs_lob_emblem') }}

),

drg_max_version as (
  select 
    drg_code, 
    drg_type, 
    max(drg_version) as drg_version
  from {{ source('claims_mappings', 'cu_drg_codes') }} 
  group by 1,2
),

drg_ref as (
  select *
  from {{ source('claims_mappings', 'cu_drg_codes') }} 
  inner join drg_max_version 
    using (drg_code, drg_type, drg_version)
)

select
    -- take all of the columns from the union except for the below
    medical_union.* except(
      patientId,
      lineOfBusiness,
      subLineOfBusiness,
      claimLineStatus
    ),

    coalesce(medical_union.patientId, 'deidentified') as patientId,
    
    case
      when 
        claimLineStatus = 'P' or
        (
          (claimLineStatus is null or claimLineStatus = 'Unknown') and
          partner = 'carefirst'
        ) then 'Paid'
      when claimLineStatus = 'D' then 'Denied'
      else claimLineStatus
    end as claimLineStatus,

    concat(costClassification1, " / ",costClassification2) as finalClassification,

    case when partner = 'emblem' then
      case when coalesce(lob_emblem.lineOfBusiness1, medical_union.lineOfBusiness) = 'DSNP' then 'Medicare'
           else coalesce(lob_emblem.lineOfBusiness1, medical_union.lineOfBusiness)
      end
    else medical_union.lineOfBusiness end as lineOfBusiness,

    case when partner = 'emblem' then
      case when coalesce(lob_emblem.lineOfBusiness2, medical_union.subLineOfBusiness) IN ('HMO', 'POS') then 'Fully Insured'
           when coalesce(lob_emblem.lineOfBusiness2, medical_union.subLineOfBusiness) = 'MedicareAdvantage' then 'Medicare Advantage'
           else coalesce(lob_emblem.lineOfBusiness2, medical_union.subLineOfBusiness)
      end
    else medical_union.lineOfBusiness end as subLineOfBusiness,

    coalesce(
      prov_serv.name,
      coalesce(
        concat(npi_serv.Provider_First_Name, " ", npi_serv.Provider_Last_Name_Legal_Name),
        npi_serv.Provider_Other_Organization_Name, npi_serv.Provider_Organization_Name_Legal_Business_Name
      )
    ) as providerServicingName,

    coalesce(prov_serv.npi, medical_union.providerServicingId) as providerServicingNPI,

   -- servcing / prescribing provider info
    coalesce(prov_serv.phone, npi_serv.Provider_Business_Practice_Location_Address_Telephone_Number) as providerServicingPhone,
    coalesce(prov_serv.email) as providerServicingEmail,
    coalesce(prov_serv.address1, npi_serv.Provider_First_Line_Business_Practice_Location_Address) as providerServicingAddress1,
    coalesce(prov_serv.address2, npi_serv.Provider_Second_Line_Business_Practice_Location_Address) as providerServicingAddress2,
    coalesce(prov_serv.city, npi_serv.Provider_Business_Practice_Location_Address_City_Name) as providerServicingCity,
    coalesce(prov_serv.state, npi_serv.Provider_Business_Practice_Location_Address_State_Name) as providerServicingState,
    substr(coalesce(prov_serv.zip, npi_serv.Provider_Business_Practice_Location_Address_Postal_Code),0,5) as providerServicingZip,

coalesce(
      --prov_pharmacy.name, -- pharmacy = billing, prescriber = servicing
      prov_bill.name,
      coalesce(
        concat(npi_bill.Provider_First_Name, " ", npi_bill.Provider_Last_Name_Legal_Name),
        npi_bill.Provider_Other_Organization_Name, npi_bill.Provider_Organization_Name_Legal_Business_Name
      ),
      coalesce(
        concat(npi_pharmacy.Provider_First_Name, " ", npi_pharmacy.Provider_Last_Name_Legal_Name),
        npi_pharmacy.Provider_Other_Organization_Name, npi_pharmacy.Provider_Organization_Name_Legal_Business_Name
      )
    ) as providerBillingName,

    coalesce(medical_union.npi, prov_bill.npi, medical_union.providerBillingId) as providerBillingNPI,

   -- billing provider / pharmacy info
    coalesce(prov_bill.phone, npi_bill.Provider_Business_Practice_Location_Address_Telephone_Number, npi_pharmacy.Provider_Business_Practice_Location_Address_Telephone_Number) as providerBillingPhone,
    coalesce(prov_bill.email) as providerBillingEmail,
    coalesce(prov_bill.address1, npi_bill.Provider_First_Line_Business_Practice_Location_Address, npi_pharmacy.Provider_First_Line_Business_Practice_Location_Address) as providerBillingAddress1,
    coalesce(prov_bill.address2, npi_bill.Provider_Second_Line_Business_Practice_Location_Address, npi_pharmacy.Provider_Second_Line_Business_Practice_Location_Address) as providerBillingAddress2,
    coalesce(prov_bill.city, npi_bill.Provider_Business_Practice_Location_Address_City_Name, npi_pharmacy.Provider_Business_Practice_Location_Address_City_Name) as providerBillingCity,
    coalesce(prov_bill.state, npi_bill.Provider_Business_Practice_Location_Address_State_Name, npi_pharmacy.Provider_Business_Practice_Location_Address_State_Name) as providerBillingState,
    substr(coalesce(prov_bill.zip, npi_bill.Provider_Business_Practice_Location_Address_Postal_Code, npi_pharmacy.Provider_Business_Practice_Location_Address_Postal_Code),0,5) as providerBillingZip,

    stayGroup as admissionId,

    case when (costClassification1 = 'SNF' and costClassification2 like 'Facility%') or
              (costClassification1 like '%IP%' and costClassification2 like '%Facility%') 
              then date_diff(coalesce(if((extract(year from dateDischarge)<2016) or (dateDischarge>current_date('EST'))
                                         ,null,dateDischarge) ,
                                      dateTo),
                             coalesce(if((extract(year from dateAdmit)<2016) or (dateAdmit>current_date('EST'))
                                         ,null,dateAdmit),
                                      dateFrom),day) end 
                              as lengthOfStay,

    hcpcs.cpt_hcpcs_desc as procedureDescription,
    hcpcs.cpt_hcpcs_major_cat as procedureCategory,
    rev.rev_code_desc as revenueDescription,
    icdpx.icd10_px_code_desc as principalProcedureDescription,
    spec.prvdr_spec_cd_desc as providerServicingSpecialtyDescription,
    icdcm.name as principalDiagnosisDescription,
    pos.pos_desc_lo as placeOfServiceDescription,
    bill_type.name as billTypeDescription,
    dsch.dis_stat_desc as dischargeStatusDescription,
    drg.drg_desc as drgDescription,

    mem.dateOfBirth,
    mem.dateOfDemise as dateOfDeath,

    current_date() as runDate

from medical_union

left join {{ source('claims_descriptions', 'cpt_hcpcs_lo_cu_v1') }} as hcpcs
  on medical_union.procedureCode = hcpcs.cpt_hcpcs_code

left join {{ source('claims_descriptions', 'rev_codes_lo') }} as rev
  on medical_union.revenueCode = rev.rev_code

left join {{ source('claims_descriptions', 'icd10_px_codes_lo') }} as icdpx
  on medical_union.principalProcedureCode = icdpx.icd10_px_code

left join {{ source('claims_descriptions', 'prvdr_spec_codes_lo') }} as spec
  on medical_union.providerServicingSpecialty = spec.prvdr_spec_cd

left join {{ source('codesets', 'icd10cm') }} as icdcm
  on medical_union.principalDiagnosisCode = icdcm.code

left join {{ source('claims_descriptions', 'pos_codes_lo') }} as pos
  on medical_union.placeOfService = pos.pos_code_lo

left join {{ source('codesets', 'ub04_bill_type') }} as bill_type
  on medical_union.billType = bill_type.code

left join {{ source('claims_descriptions', 'dsch_stat_codes_lo') }} as dsch
  on medical_union.dischargeStatus = dsch.dis_stat_code

left join drg_ref as drg
  on medical_union.drgCode = drg.drg_code

-- join to gold claims provider unioned rosters
left join gold_provider as prov_bill
  on prov_bill.id = medical_union.providerBillingId

left join gold_provider as prov_serv
  on prov_serv.id = medical_union.providerServicingId 

-- join to gold rosters on npi for pharmacy only (rx claims contain pharmacy npi)
-- removing this join because it caused fan-out where the same npi has multiple addresses
-- left join gold_provider as prov_pharmacy
--   on prov_pharmacy.npi = medical_union.npi 
--   and medical_union.claimType = 'pharmacy'

-- join to nppes npi database as a backup for provider info not included in the partner in-network rosters (however, only applies where we have npi's on the claims or from the partner rosters)
left join {{ source('nppes', 'npidata_20190512') }} as npi_bill
  on medical_union.providerBillingId = cast(npi_bill.NPI as string)

left join {{ source('nppes', 'npidata_20190512') }} as npi_serv
  on medical_union.providerServicingId = cast(npi_serv.NPI as string)

left join {{ source('nppes', 'npidata_20190512') }} as npi_pharmacy
  on medical_union.npi = cast(npi_pharmacy.NPI as string)

left join member_demographics as mem
  on mem.patientId = medical_union.patientId

-- Hotfix for LOB
left join lob_emblem
  on lob_emblem.patientId = medical_union.patientId
    and lob_emblem.spanFromDate = DATE_TRUNC(dateFrom, MONTH)

