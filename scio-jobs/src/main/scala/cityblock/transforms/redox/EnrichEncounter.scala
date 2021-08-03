package cityblock.transforms.redox

import cityblock.ehrmodels.redox.service.RedoxApiQueryResponse
import cityblock.streaming.jobs.aggregations.Encounters._
import cityblock.utilities.{Environment, Loggable}
import com.github.vitalsoftware.scalaredox.models.VisitQuery
import com.github.vitalsoftware.scalaredox.client.RedoxResponse
import play.api.libs.json.JsValue

import scala.concurrent._

trait Details extends Loggable {
  def addDetailsToEncounterReason(
    oldEncounter: Encounter,
    visitResponse: Future[RedoxResponse[JsValue]]
  )(implicit externalId: String): Encounter = {
    val encounterDetails = new EncounterDetails(oldEncounter, visitResponse)

    val reasonsAlreadyExist = oldEncounter.reasons.nonEmpty
    val hasNewElementsToAdd = encounterDetails.newNote.isDefined || encounterDetails.newReasonForVisit.isDefined

    val updatedReasonsNotes: List[EncounterReason] =
      if (!reasonsAlreadyExist && hasNewElementsToAdd) {
        List(
          EncounterReason(
            code = None,
            codeSystem = None,
            codeSystemName = None,
            name = encounterDetails.newReasonForVisit,
            notes = encounterDetails.newNote
          )
        )
      } else {
        oldEncounter.reasons.map(
          reason =>
            encounterDetails.nameLens.set(encounterDetails.newReasonForVisit)(
              encounterDetails.notesLens.set(encounterDetails.newNote)(reason)
          )
        )
      }

    encounterDetails.encounterReasonLens.set(updatedReasonsNotes)(oldEncounter)
  }
}

object EnrichEncounter extends Details with PrepareQuery with Loggable {
  def addEncounterDetails(externalId: String, encounter: Encounter)(
    implicit environment: Environment
  ): Encounter = {
    val visitJsonQueryString =
      prepareVisitQueryString(externalId, encounter.dateTime, encounter.endDateTime)
    val visitQuery = RedoxApiQueryResponse.createQuery[VisitQuery](visitJsonQueryString)
    logger.info(s"""Attempting visit query for [externalId: $externalId]""")
    val visitResp = RedoxApiQueryResponse.redoxClient.get[VisitQuery, JsValue](visitQuery)
    addDetailsToEncounterReason(encounter, visitResp)(externalId)
  }
}
