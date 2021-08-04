package cityblock.transforms.carefirst.gold

import cityblock.models.CarefirstSilverClaims.SilverLabResult
import cityblock.models.carefirst.silver.LabResult.ParsedLabResult
import cityblock.models.{Identifier, Surrogate}
import cityblock.models.gold.Claims.{LabResult, MemberIdentifier, Procedure}
import cityblock.models.gold.enums.ProcedureTier
import cityblock.transforms.Transform
import cityblock.transforms.Transform.{CodeSet, Transformer}
import cityblock.utilities.{Conversions, Loggable}
import com.spotify.scio.values.SCollection

case class LabResultTransformer(labs: SCollection[SilverLabResult]) extends Transformer[LabResult] {
  override def transform()(implicit pr: String): SCollection[LabResult] =
    labs
      .withName("Add surrogates")
      .map {
        Transform.addSurrogate(pr, "silver_claims", "lab_result", _)(_.identifier.surrogateId)
      }
      .map {
        case (surrogate, silver) => LabResultTransformer.mkLabResult(surrogate, silver)
      }
}

object LabResultTransformer extends Loggable {
  private def mkLabResult(surrogate: Surrogate, silver: SilverLabResult): LabResult =
    LabResult(
      identifier = Identifier(
        id = Transform.generateUUID(),
        partner = partner,
        surrogate = surrogate
      ),
      memberIdentifier = MemberIdentifier(
        commonId = silver.patient.source.commonId,
        partnerMemberId = silver.patient.externalId,
        patientId = silver.patient.patientId,
        partner = partner
      ),
      name = silver.data.LOCAL_TEST_NAME,
      result = silver.data.LOCAL_TEST_RESULT,
      resultNumeric = mkResultNumeric(silver.data),
      units = silver.data.LOCAL_TEST_UNITS,
      loinc = mkLoinc(silver.data),
      date = silver.data.TEST_DATE,
      procedure = mkProcedure(surrogate, silver.data)
    )

  private def mkResultNumeric(lab: ParsedLabResult): Option[BigDecimal] =
    lab.LOCAL_TEST_RESULT.flatMap(Conversions.safeParse(_, BigDecimal(_)))

  private def mkLoinc(lab: ParsedLabResult): Option[String] =
    lab match {
      case l if l.LOCALORDERCODE.nonEmpty => l.LOCALORDERCODE
      case l if l.NAT_ORDER_CODE.nonEmpty => l.NAT_ORDER_CODE
      case _                              => None
    }

  private def mkProcedure(surrogate: Surrogate, lab: ParsedLabResult): Option[Procedure] =
    for (code <- lab.PROCCODE1)
      yield
        Procedure(
          surrogate = surrogate,
          tier = ProcedureTier.Secondary.name,
          codeset = CodeSet.CPT.toString,
          code = code,
          modifiers = List()
        )
}
