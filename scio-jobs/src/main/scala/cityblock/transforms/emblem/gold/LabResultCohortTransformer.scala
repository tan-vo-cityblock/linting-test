package cityblock.transforms.emblem.gold

import cityblock.models.EmblemSilverClaims.SilverLabResultCohort
import cityblock.models.gold.Claims.{LabResult, MemberIdentifier, Procedure}
import cityblock.models.gold.enums.ProcedureTier
import cityblock.models.{Identifier, Surrogate}
import cityblock.parsers.emblem.LabResultCohort.ParsedLabResultCohort
import cityblock.transforms.Transform
import cityblock.transforms.Transform.{CodeSet, Transformer}
import cityblock.utilities.{Conversions, Loggable}
import com.spotify.scio.values.SCollection
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

import scala.util.Try

case class LabResultCohortTransformer(results: SCollection[SilverLabResultCohort])
    extends Transformer[LabResult] {
  override def transform()(implicit pr: String): SCollection[LabResult] =
    results
      .withName("Add surrogates")
      .map {
        Transform.addSurrogate(pr, "silver_claims", "lab_results", _)(_.identifier.surrogateId)
      }
      .withName("Construct lab results")
      .map {
        case (surrogate, silver) =>
          LabResultCohortTransformer.labResult(surrogate, silver)
      }
}

object LabResultCohortTransformer extends Loggable {
  private def labResult(
    surrogate: Surrogate,
    silver: SilverLabResultCohort
  ): LabResult =
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
      name = silver.result.LOCALTESTNAME,
      result = silver.result.LOCALTESTRESULT,
      resultNumeric = resultNumeric(silver.result),
      units = silver.result.LOCALTESTUNITS,
      loinc = loinc(silver.result),
      date = silver.result.LABDATE.map(date => stringToLocalDate(date)),
      procedure = procedure(surrogate, silver.result)
    )

  private def resultNumeric(result: ParsedLabResultCohort): Option[BigDecimal] =
    result.LOCALTESTRESULT.flatMap(Conversions.safeParse(_, BigDecimal(_)))

  private def loinc(result: ParsedLabResultCohort): Option[String] =
    result match {
      case r if r.LOCALORDERCODE.nonEmpty => r.LOCALORDERCODE
      case r if r.NATLORDERCODE.nonEmpty  => r.NATLORDERCODE
      case _                              => None
    }

  private def procedure(surrogate: Surrogate, result: ParsedLabResultCohort): Option[Procedure] =
    for (code <- result.PROCCODE1)
      yield
        Procedure(
          surrogate = surrogate,
          tier = ProcedureTier.Secondary.name,
          codeset = CodeSet.CPT.toString,
          code = code,
          modifiers = List()
        )

  private def stringToLocalDate(date: String): LocalDate = {
    val dt = DateTimeFormat.forPattern("dd-MMM-YY")
    Try(LocalDate.parse(date, dt)).toOption
      .getOrElse(LocalDate.parse("1900-01-01"))
  }
}
