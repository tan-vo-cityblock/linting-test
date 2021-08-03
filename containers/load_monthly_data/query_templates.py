def flatten_gold_facility(table_reference: str, partner: str) -> str:
    where_clause = """
    where date.from >= '2017-01-01'
    """ if partner is 'cci' else ''
    return """
with 

expanded_records as (
    select
        claimId,
        memberIdentifier.*,
        header.*,
        lines

    from {ref}
)

    select
        claimId,
        commonId,
        partnerMemberId,
        patientId,
        partner,
        partnerClaimId,
        lineOfBusiness,
        subLineOfBusiness,

        -- These three changed in latest gold refresh. Will have to update on Looker end to use new names
        typeOfBill,
        admissionType	 as admitType,
        admissionSource as admitSource,

        dischargeStatus,

        DRG.version as drgVersion,
        DRG.codeset as drgCodeset,
        DRG.code as drgCode,

        provider.billing.id as providerBillingId,
        provider.billing.specialty as providerBillingSpecialty,

        provider.referring.id as providerReferringId,
        provider.referring.specialty as providerReferringSpecialty,

        provider.servicing.id as providerServicingId,
        provider.servicing.specialty as providerServicingSpecialty,

        provider.operating.id as providerOperatingId,
        provider.operating.specialty as providerOperatingSpecialty,

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
        date.admit as dateAdmit,
        date.discharge as dateDischarge,

        un_lines.amount.allowed as amountAllowed,
        un_lines.amount.billed as amountBilled,
        un_lines.amount.COB as amountCOB,
        un_lines.amount.copay as amountCopay,
        un_lines.amount.deductible as amountDeductible,
        un_lines.amount.coinsurance as amountCoinsurance,
        un_lines.amount.planPaid as amountPlanPaid,

        un_lines.procedure.code as procedureCode,

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


    from expanded_records

    left join unnest(lines) as un_lines

    left join unnest(expanded_records.diagnoses) as un_header_dx
      on un_header_dx.tier = 'principal'

    left join unnest(expanded_records.procedures) as un_header_pc
      on un_header_pc.tier = 'principal'

    {where}
""".format(ref=table_reference, where=where_clause)


def flatten_gold_professional(table_reference: str, partner: str) -> str:
    dx_code_field = "un_lines_dx.code" if partner is 'emblem' else 'un_header_dx.code'
    extra_join = """
        left join unnest(expanded_records.diagnoses) as un_header_dx on un_header_dx.tier = 'principal'
    """ if partner is not 'emblem' else ''
    where_clause = """
    where un_lines.date.from >= "2017-01-01"'
    """ if partner is 'cci' else ''
    return """
with

expanded_records as (
    select
        claimId,
        memberIdentifier.*,
        header.*,
        lines
    from {ref}
)

    select
        claimId,
        commonId,
        partnerMemberId,
        patientId,
        partner,
        partnerClaimId,
        lineOfBusiness,
        subLineOfBusiness,

        expanded_records.provider.billing.id as providerBillingId,
        expanded_records.provider.billing.specialty as providerBillingSpecialty,

        expanded_records.provider.referring.id as providerReferringId,
        expanded_records.provider.referring.specialty as providerReferringSpecialty,

        un_lines.provider.servicing.id as providerServicingId,
        un_lines.provider.servicing.specialty as providerServicingSpecialty,

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

        un_lines.amount.allowed as amountAllowed,
        un_lines.amount.billed as amountBilled,
        un_lines.amount.COB as amountCOB,
        un_lines.amount.copay as amountCopay,
        un_lines.amount.deductible as amountDeductible,
        un_lines.amount.coinsurance as amountCoinsurance,
        un_lines.amount.planPaid as amountPlanPaid,

        un_lines.procedure.codeset as procedureCodeset,
        un_lines.procedure.code as procedureCode,

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
        {dx_code_field}
        as principalDiagnosisCode,

        un_lines.surrogate.id as surrogateId,
        un_lines.surrogate.project as surrogateProject,
        un_lines.surrogate.dataset as surrogateDataset,
        un_lines.surrogate.table as surrogateTable,

        un_lines.diagnoses as lineDiagnoses

    from expanded_records

    left join unnest(lines) as un_lines
    left join unnest(un_lines.diagnoses) as un_lines_dx

    {extra_join}

    {where_clause}
""".format(ref=table_reference, dx_code_field=dx_code_field, extra_join=extra_join,
           where_clause=where_clause)


def flatten_gold_pharmacy(table_reference: str, partner: str) -> str:
    return """
with
expanded_records as (
    select
        memberIdentifier.patientId as patientId,
        memberIdentifier.commonId as commonId,
        memberIdentifier.partnerMemberId,
        memberIdentifier.partner,

        identifier.id as claimId,
        identifier.surrogate.id as surrogateId,
        identifier.surrogate.project as surrogateProject,
        identifier.surrogate.dataset as surrogateDataset,
        identifier.surrogate.table as surrogateTable,
        cast(null as string) as partnerClaimId,

        lineOfBusiness,
        subLineOfBusiness,

        claimLineStatus,

        date.filled as dateFilled,
        date.paid as datePaid,

        amount.allowed as amountAllowed,
        amount.planPaid as amountPlanPaid,

        pharmacy.npi as pharmacyNpi,
        pharmacy.id as pharmacyId,

        prescriber.npi as prescriberNpi,
        prescriber.id as prescriberId,
        prescriber.specialty as prescriberSpecialty,

        drug.ndc,
        drug.daysSupply,
        drug.quantityDispensed,
        drug.dispenseAsWritten,
        drug.dispenseMethod,
        drug.fillNumber,
        drug.formularyFlag,
        drug.partnerPrescriptionNumber,
        drug.brandIndicator,
        drug.ingredient,
        case
          when drugClass.codeset = 'GC3' then drugClass.code
          else null
          end as gc3code,

        amount.deductible,
        amount.coinsurance,
        amount.copay,
        amount.cob

    from {ref} p

    left join unnest(drug.classes) as drugClass

)

-- reducing fanout from above unnest with a select distinct here
select distinct * from expanded_records
""".format(ref=table_reference)
