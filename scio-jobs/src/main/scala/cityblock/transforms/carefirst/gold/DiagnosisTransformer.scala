package cityblock.transforms.carefirst.gold

import cityblock.models.CarefirstSilverClaims.SilverDiagnosisAssociation
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
        Transform.addSurrogate(project, "silver_claims", "diagnosis_association", silver)(
          _.identifier.surrogateId)
      for (diagnosis <- mkDiagnosis(surrogate, silver)) yield (ClaimKey(silver), diagnosis)
    }.groupByKey

  def mkDiagnosis(surrogate: Surrogate, silver: SilverDiagnosisAssociation): Option[Diagnosis] =
    for (code <- silver.data.ICD_DIAG_CODE)
      yield
        Diagnosis(
          surrogate = surrogate,
          tier = DiagnosisTier.Secondary.name,
          codeset = getICDCodeset(silver.data.ICD_VERS_IND).toString,
          code = code
        )

  private def getICDCodeset(icdVersionInd: Option[String]): CodeSet =
    if (icdVersionInd.contains("0")) CodeSet.ICD10Cm else CodeSet.ICD9Cm
}
