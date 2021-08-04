package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.MedicalICDProcedure
import cityblock.models.Surrogate
import cityblock.models.gold.Claims.Procedure
import cityblock.models.gold.enums.ProcedureTier
import cityblock.transforms.Transform.CodeSet
import cityblock.transforms.Transform.CodeSet.CodeSet
import com.spotify.scio.values.SCollection

object ProcedureTransformer {
  def indexByClaimKey(
    project: String,
    procedures: SCollection[MedicalICDProcedure]
  ): SCollection[IndexedProcedures] =
    index[MedicalICDProcedure, ClaimKey, Procedure](
      project,
      "silver_claims",
      "MedicalICDProc",
      procedures,
      _.identifier.surrogateId,
      ClaimKey.apply,
      procedure
    )

  private def procedure(surrogate: Surrogate, silver: MedicalICDProcedure): Option[Procedure] =
    for {
      code <- silver.procedure.ICDProcCd
      indicator <- silver.procedure.ICD_Indicator
      codeset <- toICDCodeSet(indicator)
    } yield
      Procedure(
        surrogate = surrogate,
        tier = tier(silver.procedure.ICDProcCategory).name,
        code = code,
        codeset = codeset.toString,
        modifiers = List()
      )

  private def toICDCodeSet(indicator: String): Option[CodeSet] =
    indicator match {
      case IndicatorIsICD10 => Some(CodeSet.ICD10Pcs)
      case _                => None
    }

  private def tier(category: Option[String]): ProcedureTier = category match {
    case Some("Principal") => ProcedureTier.Principal
    case _                 => ProcedureTier.Secondary
  }
}
