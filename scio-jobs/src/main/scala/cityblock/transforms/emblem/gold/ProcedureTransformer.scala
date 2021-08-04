package cityblock.transforms.emblem.gold

import cityblock.models.EmblemSilverClaims.SilverProcedureAssociation
import cityblock.models.Surrogate
import cityblock.models.gold.Claims.Procedure
import cityblock.models.gold.enums.ProcedureTier
import cityblock.transforms.Transform
import cityblock.transforms.Transform.CodeSet
import com.spotify.scio.values.SCollection

object ProcedureTransformer {
  def indexByClaimKey(project: String, procedures: SCollection[SilverProcedureAssociation])
    : SCollection[(ClaimKey, Iterable[Procedure])] =
    procedures.flatMap { silver =>
      val (surrogate, _) =
        Transform.addSurrogate(project, "silver_claims", "procedure_associations", silver)(
          _.identifier.surrogateId)
      for (procedure <- mkProcedure(surrogate, silver))
        yield (ClaimKey(silver), procedure)
    }.groupByKey

  private def mkProcedure(surrogate: Surrogate,
                          silver: SilverProcedureAssociation): Option[Procedure] = {
    val tier: String = {
      if (silver.procedure.SEQUENCENUMBER.contains(FirstSequenceNumber)) {
        ProcedureTier.Principal.name
      } else {
        ProcedureTier.Secondary.name
      }
    }

    for (code <- silver.procedure.PROCEDURECODE)
      yield
        Procedure(
          surrogate = surrogate,
          tier = tier,
          codeset = getICDCodeset(silver).toString,
          code = code,
          modifiers = List()
        )
  }

  private def getICDCodeset(silver: SilverProcedureAssociation): CodeSet.Value =
    silver.procedure.ICDVERSION10ORHIGHER match {
      case Some("1") => CodeSet.ICD10Pcs
      case _         => CodeSet.ICD9Pcs
    }
}
