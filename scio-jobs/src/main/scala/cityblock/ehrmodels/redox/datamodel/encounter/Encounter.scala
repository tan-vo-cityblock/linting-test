package cityblock.ehrmodels.redox.datamodel
package encounter

import cityblock.utilities.backend.CirceError
import cityblock.utilities.time.DateOrInstant
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

/**
 * params - See EncountersProperties.scala and Encounters section in Redox docs for up-to-date description of
 * Encounter model fields.
 * https://developer.redoxengine.com/data-models/ClinicalSummary.html#PatientQueryResponse
 */
case class Encounter(
  Identifiers: List[EncounterIdentifier],
  Type: EncounterType,
  DateTime: Option[DateOrInstant],
  EndDateTime: Option[DateOrInstant],
  Providers: List[EncounterProvider],
  Locations: List[EncounterLocation],
  Diagnosis: List[EncounterDiagnosis],
  ReasonForVisit: List[EncounterReason]
) extends CCDObject

object Encounter {
  import DateOrInstantDecoder._

  implicit val encounterDecoder: Decoder[Encounter] =
    deriveDecoder[Encounter]

  def apply(encountersJson: io.circe.Json): Either[CirceError, List[Encounter]] =
    encountersJson.as[List[Encounter]] match {
      case Right(encounter) => Right(encounter)
      case Left(error)      => Left(CirceError(encountersJson.toString, error))
    }
}
