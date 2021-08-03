package cityblock.transforms.emblem.gold

import cityblock.models.EmblemSilverClaims.{
  SilverDiagnosisAssociation,
  SilverDiagnosisAssociationCohort,
  SilverProfessionalClaimCohort
}
import cityblock.models.Surrogate
import cityblock.models.gold.Claims.Diagnosis
import cityblock.models.gold.enums.DiagnosisTier
import cityblock.transforms.Transform
import cityblock.transforms.Transform.CodeSet
import cityblock.transforms.Transform.CodeSet.CodeSet
import com.spotify.scio.values.SCollection

object DiagnosisTransformer {
  def indexByClaimKey(project: String, diagnoses: SCollection[SilverDiagnosisAssociation])
    : SCollection[(ClaimKey, Iterable[Diagnosis])] =
    diagnoses.flatMap { silver =>
      val (surrogate, _) =
        Transform.addSurrogate(project, "silver_claims", "diagnosis_associations", silver)(
          _.identifier.surrogateId)
      for (diagnosis <- mkDiagnosis(surrogate, silver))
        yield (ClaimKey(silver), diagnosis)
    }.groupByKey

  def indexByClaimCohortKey(project: String,
                            diagnoses: SCollection[SilverDiagnosisAssociationCohort])
    : SCollection[(ClaimKey, Iterable[Diagnosis])] =
    diagnoses.flatMap { silver =>
      val (surrogate, _) =
        Transform.addSurrogate(project, "silver_claims", "diagnosis_associations", silver)(
          _.identifier.surrogateId)
      for (diagnosis <- mkDiagnosis(surrogate, silver))
        yield (ClaimKey(silver), diagnosis)
    }.groupByKey

  private def mkDiagnosis(surrogate: Surrogate,
                          silver: SilverDiagnosisAssociation): Option[Diagnosis] =
    for (code <- silver.diagnosis.DIAGNOSISCODE)
      yield
        Diagnosis(
          surrogate = surrogate,
          tier = diagnosisTier(code),
          codeset = getICDCodeset(silver.diagnosis.ICD10_OR_HIGHER).toString,
          code = code
        )

  private def mkDiagnosis(surrogate: Surrogate,
                          silver: SilverDiagnosisAssociationCohort): Option[Diagnosis] =
    for (code <- silver.diagnosis.DIAGNOSISCODE)
      yield
        Diagnosis(
          surrogate = surrogate,
          tier = diagnosisTier(code),
          codeset = getICDCodeset(silver.diagnosis.ICD10_OR_HIGHER).toString,
          code = code
        )

  private def diagnosisTier(diagnosisCode: String): String =
    diagnosisCode match {
      case "ICD_DIAG_01"    => DiagnosisTier.Principal.name
      case "ICD_DIAG_ADMIT" => DiagnosisTier.Admit.name
      case _                => DiagnosisTier.Secondary.name
    }

  private def getICDCodeset(ICD10_OR_HIGHER: Boolean): CodeSet =
    if (ICD10_OR_HIGHER) CodeSet.ICD10Cm else CodeSet.ICD9Cm

  def getICDCodeset(silver: SilverProfessionalClaimCohort): CodeSet =
    silver.claim.ICD10_OR_HIGHER.fold(CodeSet.ICD9Cm)(getICDCodeset)
}
