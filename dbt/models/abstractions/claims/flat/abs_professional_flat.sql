
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

    from {{ source( source_name, 'Professional') }}

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
        coalesce( nullif(cf_hist.HeaderBillingProviderNPI,'Unknown'), {{ source_name }}_expanded_records.provider.billing.id ) as providerBillingId,
        coalesce( tax_billing.medicareSpecialty, {{ source_name }}_expanded_records.provider.billing.specialty ) as providerBillingSpecialty,
        
        {{ source_name }}_expanded_records.provider.referring.id as providerReferringId,
        {{ source_name }}_expanded_records.provider.referring.specialty as providerReferringSpecialty,
        
        coalesce( nullif(cf_hist.RenderingProviderNPI,'Unknown'), un_lines.provider.servicing.id ) as providerServicingId,
        coalesce( tax_rendering.medicareSpecialty, un_lines.provider.servicing.specialty ) as providerServicingSpecialty,
        
        {% else -%}
        {{ source_name }}_expanded_records.provider.billing.id as providerBillingId,
        {{ source_name }}_expanded_records.provider.billing.specialty as providerBillingSpecialty,
        
        {{ source_name }}_expanded_records.provider.referring.id as providerReferringId,
        {{ source_name }}_expanded_records.provider.referring.specialty as providerReferringSpecialty,
        
        un_lines.provider.servicing.id as providerServicingId,
        un_lines.provider.servicing.specialty as providerServicingSpecialty,
        {% endif -%}

        un_lines.lineNumber,
        un_lines.surrogate.id as lineId,

        un_lines.COBFlag,
        un_lines.capitatedFlag,
        un_lines.claimLineStatus,
        un_lines.inNetworkFlag,
        un_lines.serviceQuantity,
        un_lines.placeOfService,
        un_lines.typesOfService,

        un_lines.date.from as dateFrom,
        un_lines.date.to as dateTo,
        un_lines.date.paid as datePaid,

        {% if source_name == "carefirst" -%}
        coalesce(un_lines.amount.allowed, fee.value) as amountAllowed,       
        {% else -%}
        un_lines.amount.allowed as amountAllowed,
        {% endif -%}
        un_lines.amount.billed as amountBilled,
        un_lines.amount.COB as amountCOB,
        un_lines.amount.copay as amountCopay,
        un_lines.amount.deductible as amountDeductible,
        un_lines.amount.coinsurance as amountCoinsurance,
        {% if source_name == "carefirst" -%}
        coalesce(un_lines.amount.planPaid, fee.value, 0) as amountPlanPaid,       
        {% else -%}
        un_lines.amount.planPaid as amountPlanPaid,
        {% endif -%}
        un_lines.procedure.codeset as procedureCodeset,
        nullif(un_lines.procedure.code,'Unknown') as procedureCode,

        case
          when array_length(un_lines.procedure.modifiers) > 0 then un_lines.procedure.modifiers[OFFSET(0)]
          else null
          end
        as procedureModifier1,

        case
          when array_length(un_lines.procedure.modifiers) = 2 then un_lines.procedure.modifiers[OFFSET(1)]
          else null
          end
        as procedureModifier2,

      --   expanded_records.diagnoses as headerDiagnoses,
        {% if "emblem" in source_name -%}
        {# account for both emblem and emblem virtual #}
        un_lines_dx.code
        {% else -%}
        un_header_dx.code
        {% endif -%}
        as principalDiagnosisCode,

        un_lines.surrogate.id as surrogateId,
        un_lines.surrogate.project as surrogateProject,
        un_lines.surrogate.dataset as surrogateDataset,
        un_lines.surrogate.table as surrogateTable,

        un_lines.diagnoses as lineDiagnoses

    from {{ source_name }}_expanded_records

    left join unnest(lines) as un_lines
    left join unnest(un_lines.diagnoses) as un_lines_dx

    {% if "emblem" not in source_name -%}
    {# account for both emblem and emblem virtual #}
    left join unnest({{ source_name }}_expanded_records.diagnoses) as un_header_dx on un_header_dx.tier in ('principal', 'principle')
    {% endif -%}

    {% if source_name == "carefirst" -%}
    left join (select code, max(value) as value from  {{ source( 'uploads', 'dc_medicaid_fee_schedule_cpt_hcpcs_mod_20201001') }} group by 1) as fee
      on un_lines.procedure.code = trim(fee.code)
      and un_lines.date.from < '2020-10-01'
    left join (select ClaimTCN, cast(max(HeaderBillingProviderNPI) as string) as HeaderBillingProviderNPI, cast(max(RenderingProviderNPI) as string) as RenderingProviderNPI from {{ source( 'carefirst_historic', 'HistoricalClaimData_20201002') }} group by 1) cf_hist
      on partnerClaimId = cast(cf_hist.ClaimTCN as string)
      and un_lines.date.from < '2020-10-01'
    left join {{ source('nppes', 'npidata_20190512') }} nppes_billing
      on cf_hist.HeaderBillingProviderNPI = cast(nppes_billing.NPI as string)
    left join {{ source('nppes', 'npidata_20190512') }} nppes_rendering
      on cf_hist.RenderingProviderNPI = cast(nppes_rendering.NPI as string)  
    left join {{ source('codesets', 'taxonomy_medicare_specialties') }} as tax_billing
      on nppes_billing.Healthcare_Provider_Taxonomy_Code_1 = tax_billing.Code
    left join {{ source('codesets', 'taxonomy_medicare_specialties') }} as tax_rendering
      on nppes_rendering.Healthcare_Provider_Taxonomy_Code_1 = tax_rendering.Code
    where un_lines.procedure.code not like 'D%' 
    {% endif -%}

    {% if source_name == "cci" -%}
    where un_lines.date.from >= '2017-01-01'
    {% endif -%}
)
{% if not loop.last -%} , {%- endif %}
{% endfor %}

{% for source_name in payer_list -%}
select * from {{ source_name }}_flattened
{% if not loop.last -%} union all {%- endif %}
{% endfor %}
