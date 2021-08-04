package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.FacetsMedical
import cityblock.models.Surrogate
import cityblock.models.connecticare.silver.FacetsMedical.ParsedFacetsMedical
import cityblock.models.gold.Claims.{ClaimLineStatus, Diagnosis, MemberIdentifier}
import cityblock.models.gold.TypeOfService
import cityblock.models.gold.enums.DiagnosisTier
import cityblock.transforms.Transform
import cityblock.transforms.Transform.CodeSet
import cityblock.utilities.Insurance.{LineOfBusiness, SubLineOfBusiness}
import cityblock.utilities.Loggable
import com.spotify.scio.values.SCollection

import scala.util.control.NonFatal

trait MedicalFacetsMapping extends Loggable {
  def mkLineNumber(silver: ParsedFacetsMedical): Option[Int] = {
    val claimNumber: String = silver.EXT_1001_CLAIM_NUMBER
    val claimNumberLength: Int = claimNumber.length

    try {
      Some(
        claimNumber
          .substring(claimNumberLength - FacetsMedicalClaimLineNumberLength, claimNumberLength)
          .toInt)
    } catch {
      case NonFatal(_) =>
        logger.error(s"Could not parse lineNumber from claimNumber: $claimNumber")
        None
    }
  }

  object CCIFacetsLineOrdering extends Ordering[(Surrogate, FacetsMedical)] {
    override def compare(x: (Surrogate, FacetsMedical), y: (Surrogate, FacetsMedical)): Int = {
      val l = mkLineNumber(x._2.data)
      val r = mkLineNumber(y._2.data)

      Transform.ClaimLineOrdering.compare(l, r)
    }
  }

  def mkClaimLineStatus(silver: ParsedFacetsMedical): Option[String] =
    silver.EXT_PAYMT_STATUS_1.map {
      case "PD" => ClaimLineStatus.Paid.toString
      case "DE" => ClaimLineStatus.Denied.toString
      case "VO" => ClaimLineStatus.Voided.toString
      case "AD" => ClaimLineStatus.Adjusted.toString
      case "RI" => ClaimLineStatus.Reissue.toString
      case _    => ClaimLineStatus.Unknown.toString
    }

  def mkInNetworkFlag(surrogate: Surrogate, silver: ParsedFacetsMedical): Option[Boolean] =
    silver.EXT_PAR_IND.flatMap {
      case "P" => Some(true)
      case "N" => Some(false)
      case _ =>
        val surrogateId: String = surrogate.id
        logger.error(s"Could not parse inNetworkFlag for claim line with surrogate $surrogateId")
        None
    }

  def mkTypesOfService(silver: ParsedFacetsMedical): List[TypeOfService] =
    silver.EXT_1206_TYPE_OF_SERVICE
      .map(code => List(TypeOfService(tier = 1, code = code)))
      .getOrElse(List.empty)

  def addSurrogate(
    project: String,
    table: String,
    claims: SCollection[FacetsMedical]
  ): SCollection[(Surrogate, FacetsMedical)] =
    claims.map(
      claim =>
        Transform.addSurrogate(project, "silver_claims_facets", table, claim)(
          _.identifier.surrogateId))

  def mkLineOfBusiness(surrogate: Surrogate, lineOfBusiness: Option[String]): Option[String] =
    lineOfBusiness.flatMap {
      case "1061"                            => Option(LineOfBusiness.Medicare.toString)
      case "1021"                            => Option(LineOfBusiness.Medicaid.toString)
      case "1022" | "1053" | "1056" | "1066" => Option(LineOfBusiness.Commercial.toString)
      case _ =>
        logger.error(s"Could not parse line with line of business with surrogate $surrogate.id.)")
        None
    }

  def mkSubLineOfBusiness(surrogate: Surrogate, lineOfBusiness: Option[String]): Option[String] =
    lineOfBusiness.flatMap {
      case "1061" => Option(SubLineOfBusiness.MedicareAdvantage.toString)
      case "1021" => Option(SubLineOfBusiness.DSNP.toString)
      case _ =>
        logger.error(s"Could not parse line with line of business with surrogate $surrogate.id.)")
        None
    }

  def mkDiagnoses(
    surrogate: Surrogate,
    headerLine: ParsedFacetsMedical
  ): List[Diagnosis] = {
    val headerDiagnosis: Option[Diagnosis] = {
      headerLine.EXT_1216_PRIN_DIAG_CODE.map(code => {
        Diagnosis(
          surrogate = surrogate,
          tier = DiagnosisTier.Principal.name,
          codeset = mkDiagnosisCodeset(headerLine),
          code = code
        )
      })
    }

    val secondaryDiagnosisCodes: List[String] = List(
      headerLine.EXT_1208_ICD9_DIAG_CODE1,
      headerLine.EXT_1208_ICD9_DIAG_CODE2,
      headerLine.EXT_1208_ICD9_DIAG_CODE3,
      headerLine.EXT_1208_ICD9_DIAG_CODE4,
      headerLine.EXT_1208_ICD9_DIAG_CODE5,
      headerLine.EXT_1208_ICD9_DIAG_CODE6,
      headerLine.EXT_1208_ICD9_DIAG_CODE7,
      headerLine.EXT_1208_ICD9_DIAG_CODE8,
      headerLine.EXT_1208_ICD9_DIAG_CODE9,
      headerLine.EXT_1208_ICD9_DIAG_CODE10,
      headerLine.EXT_1208_ICD9_DIAG_CODE11,
      headerLine.EXT_1208_ICD9_DIAG_CODE12
    ).flatten

    val secondaryDiagnoses: List[Diagnosis] = secondaryDiagnosisCodes.map(code => {
      Diagnosis(
        surrogate = surrogate,
        tier = DiagnosisTier.Secondary.name,
        codeset = mkDiagnosisCodeset(headerLine),
        code = code
      )
    })

    (headerDiagnosis ++ secondaryDiagnoses).toList
  }

  def mkDiagnosisCodeset(headerLine: ParsedFacetsMedical): String =
    headerLine.ICD_INDICATOR
      .map {
        case "9" => CodeSet.ICD9Cm.toString
        case _   => CodeSet.ICD10Cm.toString
      }
      .getOrElse(CodeSet.ICD10Cm.toString)

  def mkMemberIdentifier(headerLine: FacetsMedical): MemberIdentifier =
    MemberIdentifier(
      patientId = headerLine.patient.patientId,
      partnerMemberId = headerLine.patient.externalId,
      commonId = headerLine.patient.source.commonId,
      partner = partner
    )

}
