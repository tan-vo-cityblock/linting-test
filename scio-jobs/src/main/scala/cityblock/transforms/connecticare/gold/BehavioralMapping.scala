package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.{UBH, UBHDiagnosis}
import cityblock.models.Surrogate
import cityblock.models.connecticare.silver.UBH.ParsedUBH
import cityblock.models.connecticare.silver.UBHDiagnosis.ParsedUBHDiagnosis
import cityblock.models.gold.Claims.{ClaimLineStatus, Diagnosis, Procedure}
import cityblock.models.gold.enums.{DiagnosisTier, ProcedureTier}
import cityblock.models.gold.{Amount, Constructors, TypeOfService}
import cityblock.transforms.Transform
import cityblock.transforms.Transform.CodeSet
import cityblock.utilities.Conversions
import cityblock.utilities.Insurance.{LineOfBusiness, SubLineOfBusiness}
import com.spotify.scio.values.SCollection

trait BehavioralMapping {
  def addSurrogate(
    project: String,
    dataset: String,
    table: String,
    claims: SCollection[UBH]
  ): SCollection[(Surrogate, UBH)] =
    claims.map(claim =>
      Transform.addSurrogate(project, dataset, table, claim)(_.identifier.surrogateId))

  def ubhDiagnosesByDiagnosisId(
    project: String,
    table: String,
    diagnoses: SCollection[UBHDiagnosis]
  ): SCollection[(String, Iterable[(Surrogate, UBHDiagnosis)])] =
    diagnoses
      .map { diagnosis =>
        val (surrogate, _) =
          Transform.addSurrogate(project, "silver_claims", table, diagnosis)(
            _.identifier.surrogateId)
        (surrogate, diagnosis)
      }
      .groupBy(_._2.diagnosis.AUDNBR_Header)

  def firstSilverLinesByClaimKey(
    claims: SCollection[(Surrogate, UBH)]
  ): SCollection[(ClaimKey, (Surrogate, UBH))] =
    claims
      .keyBy(claim => ClaimKey(claim._2))
      .reduceByKey(UBHLineOrdering.min)

  def headersWithDiagnoses(
    headers: SCollection[(ClaimKey, (Surrogate, UBH))],
    diagnoses: SCollection[(String, Iterable[(Surrogate, UBHDiagnosis)])]
  ): SCollection[(ClaimKey, ((ClaimKey, (Surrogate, UBH)), Iterable[(Surrogate, UBHDiagnosis)]))] =
    headers.keyBy(_._2._2.claim.AUDNBR_Header).join(diagnoses).values.keyBy(_._1._1)

  def mkAmount(line: ParsedUBH): Amount =
    Constructors.mkAmount(
      allowed = line.ALLOW_AMT,
      billed = line.CLM_AMT,
      COB = None,
      copay = line.COPAY_AMT,
      deductible = line.DED_AMT,
      coinsurance = None,
      planPaid = line.PAID_AMT
    )

  def mkDiagnoses(
    claim: ParsedUBH,
    claimSurrogate: Surrogate,
    diagnoses: Iterable[(Surrogate, UBHDiagnosis)]
  ): List[Diagnosis] = {
    val primaryDiagnosis: List[Diagnosis] = claim.PRIM_DX
      .map(
        code =>
          Diagnosis(
            surrogate = claimSurrogate,
            tier = DiagnosisTier.Principal.name,
            codeset = if (claim.ICD_CDE_IND.contains("9")) {
              CodeSet.ICD9Cm.toString
            } else {
              CodeSet.ICD10Cm.toString
            },
            code = code
        ))
      .toList

    val secondaryDiagnoses: List[Diagnosis] = {
      for {
        diagInfo <- diagnoses
      } yield {
        val surrogate = diagInfo._1
        val diagnosis: ParsedUBHDiagnosis = diagInfo._2.diagnosis
        val diagnosisDiagnosis: String = diagnosis.DiagnosisIdNum

        val (tier, code, diagnosisSurrogate): (String, String, Surrogate) = {
          if (diagnosis.DiagnosisTypeDesc.contains("Principal") && primaryDiagnosis.isEmpty) {
            (DiagnosisTier.Principal.name, diagnosisDiagnosis, surrogate)
          } else {
            (DiagnosisTier.Secondary.name, diagnosisDiagnosis, surrogate)
          }
        }

        val diagnosisCodeSet: String = {
          if (claim.ICD_CDE_IND.contains("9")) CodeSet.ICD9Cm.toString else CodeSet.ICD10Cm.toString
        }

        Diagnosis(
          surrogate = diagnosisSurrogate,
          tier = tier,
          codeset = diagnosisCodeSet,
          code = code
        )
      }
    }.toList

    secondaryDiagnoses ++ primaryDiagnosis
  }

  def mkLineOfBusiness(isMedicare: Boolean): Option[String] =
    if (isMedicare) {
      Some(LineOfBusiness.Medicare.toString)
    } else {
      Some(LineOfBusiness.Commercial.toString)
    }

  def mkSubLineOfBusiness(isMedicare: Boolean): Option[String] =
    if (isMedicare) Some(SubLineOfBusiness.MedicareAdvantage.toString) else None

  def mkProcedure(surrogate: Surrogate, line: ParsedUBH): Option[Procedure] =
    line.PROC_CD1.map(procCode => {
      val codeset: String = Transform.determineProcedureCodeset(procCode)

      Procedure(
        surrogate = surrogate,
        tier = ProcedureTier.Secondary.name,
        codeset = codeset,
        code = procCode,
        modifiers = List(line.PROC_MOD, line.PROC_CD2).flatten
      )
    })

  def mkClaimLineStatus(line: ParsedUBH): Option[String] =
    line.PAID.map {
      case "0"    => ClaimLineStatus.Denied.toString
      case "1"    => ClaimLineStatus.Paid.toString
      case status => status
    }

  def mkLineNumber(claim: ParsedUBH): Option[Int] =
    for {
      detNbr: String <- claim.Det_NBR
      lineNumber: Int <- Conversions.safeParse(detNbr, _.toInt)
    } yield {
      lineNumber
    }

  def mkInNetworkFlag(claim: ParsedUBH): Option[Boolean] =
    claim.CONTR_PROV.map(_ == "Y")

  def mkTypeOfService(claim: ParsedUBH): List[TypeOfService] =
    claim.SVC_CD.map(serviceCode => TypeOfService(1, serviceCode)).toList

  object UBHLineOrdering extends Ordering[(Surrogate, UBH)] {
    override def compare(x: (Surrogate, UBH), y: (Surrogate, UBH)): Int = {
      val l: Option[Int] = Conversions.safeParse(x._2.claim.AUDNBR, _.toInt)
      val r: Option[Int] = Conversions.safeParse(y._2.claim.AUDNBR, _.toInt)

      Transform.ClaimLineOrdering.compare(l, r)
    }
  }
}
