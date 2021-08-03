
{{ config(tags = ['payer_list']) }}

{%- set payer_list = var('payer_list') -%}

with
{% for source_name in payer_list %}
{{ source_name }}_expanded_records as (

    select
        claimId,
        memberIdentifier.*,
        header.*,
        lines

    from {{ source( source_name, 'Facility') }}

),

{{ source_name }}_flattened as (

    select
        claimId,
        commonId,
        partnerMemberId,
        patientId,
        partner,
        '{{ source_name }}' as partnerSourceName,
        partnerClaimId,
        lineOfBusiness,
        subLineOfBusiness,

        {% if source_name == "carefirst" -%}
        -- imputing fake typeofbill codes in order to flag historic carefirst inpatient facility claims in stg_facility_costs_categories_base.sql
        case 
          when date.from < '2020-10-01' and (cf_hist_clm.CategoryOfServiceDescription = 'SNF Services' or cf_hist_clm.ClaimTypeDescription like 'Nursing%') and cf_hist_clm.revenuecode like '0%' then '021X' -- maps to SNF
          when date.from < '2020-10-01' and cf_hist_clm.ClaimTypeDescription = 'Inpatient' and cf_hist_clm.revenuecode like '0%' then '011X' -- maps to IP
          when date.from < '2020-10-01' and cf_hist_clm.revenuecode like '0%' then '00XX' -- maps to OP
          when date.from >= '2020-10-01' then lpad(typeOfBill, 4, '0') -- adding leading zero to CF clm billtype as platform is not doing this in the transformation
          else null end as typeOfBill,
        {% else -%}
        typeOfBill,
        {% endif -%}

        admissionType	 as admitType,
        admissionSource as admitSource,

        dischargeStatus,

        DRG.version as drgVersion,
        DRG.codeset as drgCodeset,
        DRG.code as drgCode,

        {% if source_name == "carefirst" -%}
        coalesce( nullif(cf_hist_prov.HeaderBillingProviderNPI,'Unknown'), provider.billing.id ) as providerBillingId,
        coalesce( tax_billing.medicareSpecialty, provider.billing.specialty ) as providerBillingSpecialty,

        provider.referring.id as providerReferringId,
        provider.referring.specialty as providerReferringSpecialty,

        coalesce( nullif(cf_hist_prov.RenderingProviderNPI,'Unknown'), provider.servicing.id ) as providerServicingId,
        coalesce( tax_rendering.medicareSpecialty, provider.servicing.specialty ) as providerServicingSpecialty,

        provider.operating.id as providerOperatingId,
        provider.operating.specialty as providerOperatingSpecialty,

        {% else -%}
        provider.billing.id as providerBillingId,
        provider.billing.specialty as providerBillingSpecialty,

        provider.referring.id as providerReferringId,
        provider.referring.specialty as providerReferringSpecialty,

        provider.servicing.id as providerServicingId,
        provider.servicing.specialty as providerServicingSpecialty,

        provider.operating.id as providerOperatingId,
        provider.operating.specialty as providerOperatingSpecialty,
        {% endif -%}

        un_lines.lineNumber,
        un_lines.surrogate.id as lineId,

        un_lines.COBFlag,
        un_lines.capitatedFlag,
        un_lines.claimLineStatus,
        un_lines.inNetworkFlag,
        un_lines.serviceQuantity,
        un_lines.typesOfService,
        un_lines.revenueCode,

        date.from as dateFrom,
        date.to as dateTo,
        date.paid as datePaid,

        {% if source_name == "carefirst" -%}
        -- imputing admit/discharge dates for cf hist clms based on min/max from/to dates
        case when date.from < '2020-10-01' and cf_hist_clm.ClaimTypeDescription = 'Inpatient' and cf_hist_clm.revenuecode like '0%' then min(date.from) over (partition by claimId)
          else date.admit end as dateAdmit, 
        case when date.from < '2020-10-01' and cf_hist_clm.ClaimTypeDescription = 'Inpatient' and cf_hist_clm.revenuecode like '0%' then max(date.to) over (partition by claimId)
          else date.discharge end as dateDischarge,
        -- imputing historic inpatient facility claim proxy dollar amounts based on DCHFP SSI rate sheet pmpm for base year 2018 with 2% annual trend; otherwise using procedure code based facility fee schedule
        round(case 
          when extract(year from date.from) = 2017 and un_lines.lineNumber = 1 and cf_hist_clm.revenuecode like '0%' and cf_hist_clm.ClaimTypeDescription = 'Inpatient'
            then 707.30 * (date_diff( max(date.to) over (partition by claimId), min(date.from) over (partition by claimId) , day) +1)         
          when extract(year from date.from) = 2018 and un_lines.lineNumber = 1 and cf_hist_clm.revenuecode like '0%' and cf_hist_clm.ClaimTypeDescription = 'Inpatient'
            then 721.42 * (date_diff( max(date.to) over (partition by claimId), min(date.from) over (partition by claimId) , day) +1)
          when extract(year from date.from) = 2019 and un_lines.lineNumber = 1 and cf_hist_clm.revenuecode like '0%' and cf_hist_clm.ClaimTypeDescription = 'Inpatient'
            then 735.85 * (date_diff( max(date.to) over (partition by claimId), min(date.from) over (partition by claimId) , day) +1)
          when extract(year from date.from) = 2020 and un_lines.lineNumber = 1 and extract(month from date.from) < 10 and cf_hist_clm.revenuecode like '0%' and cf_hist_clm.ClaimTypeDescription = 'Inpatient'
            then 750.56 * (date_diff( max(date.to) over (partition by claimId), min(date.from) over (partition by claimId) , day) +1)
          else 
            coalesce(un_lines.amount.planPaid, safe_cast(trim(fee.FACILITY_PRICING) as FLOAT64), fee.value * 0.59, 0) 
          end ,2) as amountAllowed,    
        {% else -%}
        date.admit as dateAdmit,
        date.discharge as dateDischarge,
        coalesce(un_lines.amount.allowed, 0) as amountAllowed,
        {% endif -%}

        un_lines.amount.billed as amountBilled,
        un_lines.amount.COB as amountCOB,
        un_lines.amount.copay as amountCopay,
        un_lines.amount.deductible as amountDeductible,
        un_lines.amount.coinsurance as amountCoinsurance,

        {% if source_name == "carefirst" -%}
        -- imputing historic inpatient facility claim proxy dollar amounts based on DCHFP SSI rate sheet pmpm for base year 2018 with 2% annual trend; otherwise using procedure code based facility fee schedule
        round(case 
          when extract(year from date.from) = 2017 and un_lines.lineNumber = 1 and cf_hist_clm.revenuecode like '0%' and cf_hist_clm.ClaimTypeDescription = 'Inpatient'
            then 707.30 * (date_diff( max(date.to) over (partition by claimId), min(date.from) over (partition by claimId) , day) +1)         
          when extract(year from date.from) = 2018 and un_lines.lineNumber = 1 and cf_hist_clm.revenuecode like '0%' and cf_hist_clm.ClaimTypeDescription = 'Inpatient'
            then 721.42 * (date_diff( max(date.to) over (partition by claimId), min(date.from) over (partition by claimId) , day) +1)
          when extract(year from date.from) = 2019 and un_lines.lineNumber = 1 and cf_hist_clm.revenuecode like '0%' and cf_hist_clm.ClaimTypeDescription = 'Inpatient'
            then 735.85 * (date_diff( max(date.to) over (partition by claimId), min(date.from) over (partition by claimId) , day) +1)
          when extract(year from date.from) = 2020 and un_lines.lineNumber = 1 and extract(month from date.from) < 10 and cf_hist_clm.revenuecode like '0%' and cf_hist_clm.ClaimTypeDescription = 'Inpatient'
            then 750.56 * (date_diff( max(date.to) over (partition by claimId), min(date.from) over (partition by claimId) , day) +1)
          else 
            coalesce(un_lines.amount.planPaid, safe_cast(trim(fee.FACILITY_PRICING) as FLOAT64), fee.value * 0.59, 0) 
          end ,2) as amountPlanPaid,
        {% else -%}
        coalesce(un_lines.amount.planPaid, 0) as amountPlanPaid,
        {% endif -%}

        nullif(un_lines.procedure.code,'Unknown') as procedureCode,

        un_lines.procedure.modifiers[SAFE_OFFSET(0)] as procedureModifier1,
        un_lines.procedure.modifiers[SAFE_OFFSET(1)] as procedureModifier2,

        --   expanded_records.diagnoses as headerDiagnoses,
        un_header_dx.code as principalDiagnosisCode,
        --   expanded_records.procedures as headerProcedures,
        un_header_pc.code as principalProcedureCode,

        un_lines.surrogate.id as surrogateId,
        un_lines.surrogate.project as surrogateProject,
        un_lines.surrogate.dataset as surrogateDataset,
        un_lines.surrogate.table as surrogateTable

    from {{ source_name }}_expanded_records

    left join unnest(lines) as un_lines

    left join unnest({{ source_name }}_expanded_records.diagnoses) as un_header_dx
      on un_header_dx.tier in ('principal', 'principle')

    left join unnest({{ source_name }}_expanded_records.procedures) as un_header_pc
      on un_header_pc.tier in ('principal', 'principle')

    {% if source_name == "carefirst" -%}
    left join (select code, max(value) as value, max(FACILITY_PRICING) as facility_pricing from  {{ source( 'uploads', 'dc_medicaid_fee_schedule_cpt_hcpcs_mod_20201001') }} group by 1) as fee
      on un_lines.procedure.code = trim(fee.code)
      and date.from < '2020-10-01'
    
    left join (
                select 
                  ClaimTCN, 
                  cast(max(HeaderBillingProviderNPI) as string) as HeaderBillingProviderNPI, 
                  cast(max(RenderingProviderNPI) as string) as RenderingProviderNPI
                from {{ source( 'carefirst_historic', 'HistoricalClaimData_20201002') }} 
                group by 1
              ) as cf_hist_prov
        on partnerClaimId = cast(cf_hist_prov.ClaimTCN as string)
        and date.from < '2020-10-01'
    
    left join {{ source( 'carefirst_historic', 'HistoricalClaimData_20201002') }} cf_hist_clm
      on partnerClaimId = cast(cf_hist_clm.ClaimTCN as string)
      and lineNumber = LineItemNumber
      --and date.from < '2020-10-01'
    
    left join {{ source('nppes', 'npidata_20190512') }} nppes_billing
      on cf_hist_prov.HeaderBillingProviderNPI = cast(nppes_billing.NPI as string)
    
    left join {{ source('nppes', 'npidata_20190512') }} nppes_rendering
      on cf_hist_prov.RenderingProviderNPI = cast(nppes_rendering.NPI as string)  
    
    left join {{ source('codesets', 'taxonomy_medicare_specialties') }} as tax_billing
      on nppes_billing.Healthcare_Provider_Taxonomy_Code_1 = tax_billing.Code
    
    left join {{ source('codesets', 'taxonomy_medicare_specialties') }} as tax_rendering
      on nppes_rendering.Healthcare_Provider_Taxonomy_Code_1 = tax_rendering.Code
    {% endif -%}

    {% if source_name == "cci" -%}
    where date.from >= '2017-01-01'
    {% endif -%}
)
{% if not loop.last -%} , {%- endif %}
{% endfor %}

{% for source_name in payer_list -%}
select * from {{ source_name }}_flattened
{% if not loop.last -%} union all {%- endif %}
{% endfor %}
