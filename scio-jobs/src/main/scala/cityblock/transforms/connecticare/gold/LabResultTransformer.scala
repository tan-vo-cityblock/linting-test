package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.LabResults
import cityblock.models.connecticare.silver.LabResult.ParsedLabResults
import cityblock.models.gold.Claims.{LabResult, MemberIdentifier, Procedure}
import cityblock.models.gold.enums.ProcedureTier
import cityblock.models.{Identifier, Surrogate}
import cityblock.transforms.Transform
import cityblock.transforms.Transform.{CodeSet, Transformer}
import cityblock.utilities.Conversions
import com.spotify.scio.values.SCollection

case class LabResultTransformer(results: SCollection[LabResults]) extends Transformer[LabResult] {
  override def transform()(implicit pr: String): SCollection[LabResult] =
    results.map { result =>
      val (surrogate, _) =
        Transform.addSurrogate(pr, "silver_claims", "LabResults", result)(_.identifier.surrogateId)
      LabResultTransformer.labResult(surrogate, result)
    }
}

object LabResultTransformer {
  def labResult(surrogate: Surrogate, silver: LabResults): LabResult =
    LabResult(
      identifier = Identifier(
        id = Transform.generateUUID(),
        partner = partner,
        surrogate = surrogate
      ),
      memberIdentifier = MemberIdentifier(
        patientId = silver.patient.patientId,
        partnerMemberId = silver.patient.externalId,
        commonId = silver.patient.source.commonId,
        partner = partner
      ),
      name = silver.results.TestName,
      result = silver.results.ResultLiteral,
      resultNumeric = resultNumeric(silver.results),
      units = silver.results.ResultUnits,
      loinc = loinc(silver.results),
      date = silver.results.DateEff,
      procedure = procedure(surrogate, silver.results)
    )

  def resultNumeric(result: ParsedLabResults): Option[BigDecimal] =
    result.ResultLiteral.flatMap(Conversions.safeParse(_, BigDecimal(_)))

  def loinc(result: ParsedLabResults): Option[String] = {
    val code: Option[String] = result match {
      case r if r.NationalResultCode.nonEmpty => r.NationalResultCode
      case r if r.TestCode.nonEmpty           => r.TestCode
      case _                                  => None
    }

    // Remove hyphens and human readable descriptions
    code.map(_.replaceAll("\\D", ""))
  }

  def procedure(surrogate: Surrogate, result: ParsedLabResults): Option[Procedure] =
    for (code <- result.CPTCode)
      yield
        Procedure(
          surrogate = surrogate,
          tier = ProcedureTier.Secondary.name,
          codeset = CodeSet.CPT.toString,
          code = code,
          modifiers = List()
        )
}
