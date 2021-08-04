package cityblock.transforms.redox

import cityblock.ehrmodels.redox.service.RedoxApiQueryResponse
import cityblock.streaming.jobs.aggregations.Encounters.{Encounter, EncounterReason, Identifier}
import cityblock.utilities.Loggable
import com.github.vitalsoftware.scalaredox.client.RedoxResponse
import monocle.Lens
import play.api.libs.json.{JsNull, JsValue}

import scala.concurrent.Future

class EncounterDetails(oldEncounter: Encounter, visitResponse: Future[RedoxResponse[JsValue]])(
  implicit externalId: String
) extends Loggable {
  val encounterReasonLens: Lens[Encounter, List[EncounterReason]] =
    createReasonLens
  val notesLens: Lens[EncounterReason, Option[String]] = createNotesLens
  val nameLens: Lens[EncounterReason, Option[String]] = createNameLens
  private val visit = pullVisitResponse(oldEncounter.identifiers)
  val newNote: Option[String] =
    cleanPullElementFromVisitResponse("HistoryOfPresentIllnessText", visit)
  val newReasonForVisit: Option[String] =
    cleanPullElementFromVisitResponse("ReasonForVisitText", visit)

  private def pullVisitResponse(
    clinicalSummaryVisitIdentifiers: List[Identifier]
  )(implicit externalId: String): Option[JsValue] = {
    val jsResponse = {
      RedoxApiQueryResponse.awaitResponse[JsValue](visitResponse).getOrElse(JsNull)
    }
    val messageId = (jsResponse \ "Meta" \ "Message" \ "ID").asOpt[String]
    if (clinicalSummaryVisitIdentifiers.length > 1) {
      throw new UnsupportedOperationException(
        s"""FAILURE: VisitId Validation does not currently support more than
           | one element in list: [clinicalSummaryVisitIdentifiers: $clinicalSummaryVisitIdentifiers]""".stripMargin
      )
    }
    val clinicalSummaryVisitNumber = clinicalSummaryVisitIdentifiers.head.id
    clinicalSummaryVisitNumber.flatMap(
      originalVisitNumber => {
        if (validateNote(originalVisitNumber, jsResponse)) {
          logger.info(
            s"""VISIT NOTE [externalId: $externalId] FOR
            VISIT ID [VisitNumber: $clinicalSummaryVisitNumber]
            MESSAGE ID [messageId: $messageId] WAS FOUND""".stripMargin
          )
          Some(jsResponse)
        } else {
          logger.info(
            s"""VISIT NOTE [externalId: $externalId] FOR
            VISIT ID [VisitNumber: $clinicalSummaryVisitNumber]
            WAS NOT FOUND""".stripMargin
          )
          None
        }
      }
    )
  }

  private def cleanPullElementFromVisitResponse(
    jsonKey: String,
    rawResponse: Option[JsValue]
  ): Option[String] =
    rawResponse.flatMap(response => pullElementFromVisitResponse(jsonKey, response))

  private def pullElementFromVisitResponse(jsonKey: String, rawResponse: JsValue): Option[String] =
    (rawResponse \ jsonKey)
      .asOpt[String]
      .flatMap(rawNote => RedoxApiQueryResponse.cleanHtml(rawNote))

  private def validateNote(
    clinicalSummaryVisitNumber: String,
    visitResponse: JsValue
  ): Boolean = {
    val visitNumberFromResponse = (visitResponse \ "Header" \ "Document" \ "Visit" \ "VisitNumber")
      .asOpt[String]
    visitNumberFromResponse.contains(clinicalSummaryVisitNumber)
  }

  private def createNameLens: Lens[EncounterReason, Option[String]] =
    Lens[EncounterReason, Option[String]](
      _.name
    )(newName => _.copy(name = newName))

  private def createNotesLens: Lens[EncounterReason, Option[String]] =
    Lens[EncounterReason, Option[String]](
      _.notes
    )(newNotes => _.copy(notes = newNotes))

  private def createReasonLens: Lens[Encounter, List[EncounterReason]] =
    Lens[Encounter, List[EncounterReason]](
      _.reasons
    )(encounterReasons => _.copy(reasons = encounterReasons))
}
