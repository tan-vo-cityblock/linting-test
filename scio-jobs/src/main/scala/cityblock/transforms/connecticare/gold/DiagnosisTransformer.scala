package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.MedicalDiagnosis
import cityblock.models.gold.Claims.Diagnosis
import cityblock.models.Surrogate
import cityblock.models.gold.enums.DiagnosisTier
import cityblock.transforms.Transform.CodeSet
import cityblock.transforms.Transform.CodeSet.CodeSet
import com.spotify.scio.values.SCollection

object DiagnosisTransformer {
  def indexByClaimKey(
    project: String,
    diagnoses: SCollection[MedicalDiagnosis]
  ): SCollection[IndexedDiagnoses] =
    index[MedicalDiagnosis, ClaimKey, Diagnosis](
      project,
      "silver_claims",
      "MedicalDiagnosis",
      diagnoses,
      _.identifier.surrogateId,
      ClaimKey.apply,
      diagnosis
    )

  private def diagnosis(surrogate: Surrogate, diagnosis: MedicalDiagnosis): Option[Diagnosis] =
    for {
      code <- diagnosis.diagnosis.DiagCd
      codeset <- toCodeSet(diagnosis.diagnosis.ICD_Indicator)
    } yield
      Diagnosis(
        surrogate = surrogate,
        tier = tier(diagnosis.diagnosis.DiagCategory).name,
        codeset = codeset.toString,
        code = code,
      )

  private def toCodeSet(indicator: Option[String]): Option[CodeSet] =
    indicator match {
      case Some(IndicatorIsICD10) => Some(CodeSet.ICD10Cm)
      case _                      => None
    }

  private def tier(category: Option[String]): DiagnosisTier =
    category match {
      case Some("Principal") => DiagnosisTier.Principal
      case Some("Admitting") => DiagnosisTier.Admit
      case _                 => DiagnosisTier.Secondary
    }
}
