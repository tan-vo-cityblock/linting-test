package cityblock.transforms.cardinal

import java.util.UUID

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.models.CardinalSilverClaims.SilverPhysical
import cityblock.models.Surrogate
import cityblock.models.cardinal.silver.Physical.ParsedPhysical
import cityblock.models.gold.Claims.{ClaimLineStatus, Diagnosis, MemberIdentifier, Procedure}
import cityblock.models.gold.{Amount, Constructors, ProviderIdentifier}
import cityblock.models.gold.enums.{DiagnosisTier, ProcedureTier}
import cityblock.transforms.Transform
import cityblock.utilities.Insurance.LineOfBusiness
import cityblock.utilities.{Conversions, PartnerConfiguration}

package object gold {
  private[gold] val partner: String = PartnerConfiguration.cardinal.indexName

  private[gold] def mkMemberIdentifier(patient: Patient): MemberIdentifier =
    MemberIdentifier(
      commonId = patient.source.commonId,
      partnerMemberId = patient.externalId,
      patientId = patient.patientId,
      partner = partner
    )

  private[gold] def mkProviderId(idField: String): String =
    UUID.nameUUIDFromBytes(idField.getBytes()).toString

  private[gold] def isProfessional(silverPhysical: SilverPhysical): Boolean =
    silverPhysical.data.PhysicalClaimType.contains("Professional")

  private[gold] def isFacility(silverPhysical: SilverPhysical): Boolean =
    silverPhysical.data.PhysicalClaimType.contains("Facility")

  private[gold] def mkCobFlag(thirdPartyAmt: Option[String]): Option[Boolean] =
    thirdPartyAmt.flatMap(Conversions.safeParse(_, BigDecimal(_))) match {
      case Some(cob) => Some(cob != 0)
      case _         => None
    }

  private[gold] def mkLineNumber(silver: ParsedPhysical): Int =
    silver.LineNumber.flatMap(Conversions.safeParse(_, _.toInt)).getOrElse(0)

  private[gold] def claimLineStatus(status: String): String =
    status match {
      case "P" => ClaimLineStatus.Paid.toString
      case "D" => ClaimLineStatus.Denied.toString
      case _   => ClaimLineStatus.Unknown.toString
    }

  private[gold] def mkInNetworkFlag(physical: ParsedPhysical): Option[Boolean] =
    physical.HeaderStatusCode match {
      case Some("P") => Some(true)
      case Some("D") => Some(false)
      case _         => None
    }

  private[gold] def mkLineOfBusiness(silver: ParsedPhysical): Option[String] =
    if (silver.HealthPlanName.contains("MDCD")) {
      Some(LineOfBusiness.Medicaid.toString)
    } else {
      None
    }

  private[gold] def mkDiagnoses(surrogate: Surrogate, silver: ParsedPhysical): List[Diagnosis] = {
    val secondaryDiagCodes: List[String] = List(
      silver.DiagnosisCode2,
      silver.DiagnosisCode3,
      silver.DiagnosisCode4,
      silver.DiagnosisCode5,
      silver.DiagnosisCode6,
      silver.DiagnosisCode7,
      silver.DiagnosisCode8
    ).flatten

    val secondaryDiagnoses = secondaryDiagCodes.map(code => {
      Diagnosis(
        surrogate = surrogate,
        tier = DiagnosisTier.Secondary.name,
        codeset = Transform.CodeSet.ICD10Cm.toString,
        code = Transform.cleanDiagnosisCode(code)
      )
    })

    secondaryDiagnoses ++ silver.DiagnosisCode1.map { code =>
      Diagnosis(
        surrogate = surrogate,
        tier = DiagnosisTier.Principal.name,
        codeset = Transform.CodeSet.ICD10Cm.toString,
        code = Transform.cleanDiagnosisCode(code)
      )
    }
  }

  private[gold] def mkProcedure(surrogate: Surrogate, silver: SilverPhysical): Option[Procedure] = {
    val tier = if (silver.data.LineNumber.contains("1")) {
      ProcedureTier.Principal
    } else {
      ProcedureTier.Secondary
    }

    Transform.mkProcedure(
      surrogate,
      silver,
      tier,
      (s: SilverPhysical) => s.data.ProcedureCode,
      (s: SilverPhysical) =>
        (s.data.ProcedureCodeModifierCode1 ++ s.data.ProcedureCodeModifierCode2 ++ s.data.ProcedureCodeModifierCode3
          ++ s.data.ProcedureCodeModifierCode4).toList
    )
  }

  private[gold] def mkAmount(silver: ParsedPhysical): Amount =
    Constructors.mkAmount(
      allowed = silver.LineNetPayableAmount,
      billed = None, // TODO
      COB = silver.TotalThirdPartyLiabilityAmount,
      copay = None, // TODO
      deductible = None, // TODO
      coinsurance = None, // TODO
      planPaid = silver.LineNetPayableAmount
    )

  private[gold] def mkClaimProvider(maybeNpi: Option[String]): Option[ProviderIdentifier] =
    for (npi <- maybeNpi)
      yield
        ProviderIdentifier(
          id = mkProviderId(npi),
          specialty = None
        )
}
