package cityblock.aggregators.jobs

import cityblock.ehrmodels.redox.datamodel.encounter.Encounter
import cityblock.streaming.jobs.RedoxSubscriber.RedoxMessage
import cityblock.streaming.jobs.aggregations.Encounters._
import cityblock.streaming.jobs.aggregations.Medications._
import cityblock.streaming.jobs.aggregations.Problems._
import cityblock.streaming.jobs.aggregations.Results._
import cityblock.streaming.jobs.aggregations.VitalSigns._
import cityblock.streaming.jobs.common.{ParsableData, ParsedPatientMedicalDataLists}
import cityblock.transforms.redox.EncountersTransforms
import cityblock.utilities.{EhrMedicalDataIO, Environment}
import com.spotify.scio.values.SCollection
import io.circe.Json
import io.circe.optics.JsonPath.root
import io.circe.parser.parse
import org.joda.time.Instant

object ClinicalSummaryRefresher extends EhrMedicalDataIO with ParsableData {
  override def parsePatientMedicalData(
    redoxMessages: SCollection[RedoxMessage],
    isStreaming: Boolean
  )(implicit environment: Environment): ParsedPatientMedicalDataLists = {
    val problemLists = parseList[PatientProblem](redoxMessages, "Problems")
    val medicationLists =
      parseList[PatientMedication](redoxMessages, "Medications")
    val encounterLists =
      parseEncountersList(redoxMessages, isStreaming)
    val vitalSignLists =
      parseList[PatientVitalSign](redoxMessages, "VitalSigns")
    val resultLists =
      parseList[PatientResult](redoxMessages, "Results")

    ParsedPatientMedicalDataLists(
      problemLists,
      medicationLists,
      encounterLists,
      vitalSignLists,
      Some(resultLists)
    )
  }

  override def parseEncountersList(
    rows: SCollection[RedoxMessage],
    isStreaming: Boolean
  )(implicit environment: Environment): SCollection[List[PatientEncounter]] =
    rows
      .withName(s"Parse Encounter data from Streaming Messages")
      .map { message =>
        val payloadJson = parse(message.payload).getOrElse(Json.Null)

        val messageIdJsonPath = root.Meta.Message.ID.long
        val messageId = messageIdJsonPath
          .getOption(payloadJson)
          .fold(
            throw new Exception("Could not parse Redox Message Id")
          )(_.toString)

        val encountersJsonPath = root.Encounters.json
        val encountersJson =
          encountersJsonPath.getOption(payloadJson).getOrElse(Json.Null)

        // Ok to set to 0 as this ClinicalSummaryRefresher is used only in tests
        // TODO: Figure out how to not duplicate ClinicalSummaryRefresher just for tests
        val insertedAt = Instant.ofEpochMilli(0)
        val redoxEncounters = Encounter(encountersJson)
          .fold(error => throw new Exception(s"Could not parse Redox Encounter $error"), identity)
        redoxEncounters.map(
          redoxEncounter =>
            EncountersTransforms
              .transformRedoxEncounterToPatientEncounter(
                messageId,
                insertedAt,
                isStreaming,
                message.patient,
                redoxEncounter
            )
        )
      }
}
