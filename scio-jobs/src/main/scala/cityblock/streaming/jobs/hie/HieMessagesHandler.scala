package cityblock.streaming.jobs.hie

import java.text.Normalizer

import cityblock.models.gold.PriorAuthorization.Procedure
import cityblock.streaming.jobs.common.ParsableData
import cityblock.streaming.jobs.RedoxSubscriber.RedoxMessage
import cityblock.streaming.jobs.admin.Admin._
import cityblock.streaming.jobs.hie.Events._
import cityblock.streaming.jobs.notes.Notes.RawNote
import cityblock.utilities.time.DateOrInstant
import cityblock.utilities.{EhrMedicalDataIO, Environment, ResultSigner}
import com.spotify.scio.values.SCollection
import io.circe.generic.auto._

object HieMessagesHandler extends EhrMedicalDataIO with ParsableData {
  case class ParsedNoteMessage(messageId: String, message: RedoxMessage, rawNote: RawNote)
  case class ParsedAdminMessage(
    messageId: String,
    message: RedoxMessage,
    rawAdminMessage: RawAdminMessage
  )
  case class PublishablePatientHieEvent(
    patientId: String,
    source: String,
    eventDateTime: Option[DateOrInstant],
    dischargeDateTime: Option[DateOrInstant],
    notes: Option[String],
    locationName: String,
    fullLocationName: Option[String],
    eventType: Option[String],
    visitType: Option[String],
    diagnoses: List[Diagnosis],
    procedures: List[Procedure],
    facilityType: Option[String],
    admissionSource: Option[String],
    locationAddress: Option[String],
    locationCity: Option[String],
    locationZip: Option[String],
    locationState: Option[String],
    provider: Option[Provider],
    messageId: Option[String]
  )

  def processAndPersistNotesMessages(
    messages: SCollection[RedoxMessage],
    topicName: String
  )(implicit environment: Environment): Unit = {
    val parsedMessages = parsePatientNotesMessages(messages)

    savePatientHieEventsToBigQuery(parsedMessages)
    publishHieEvents(parsedMessages, topicName)
  }

  def processAndPersistPatientAdminMessages(
    messages: SCollection[RedoxMessage],
    topicName: String
  )(implicit environment: Environment): Unit = {
    val parsedMessages = parsePatientAdminMessages(messages)

    savePatientHieEventsToBigQuery(parsedMessages)
    publishHieEvents(parsedMessages, topicName)
  }

  def publishHieEvents(
    messages: SCollection[PatientHieEvent],
    topicName: String
  )(implicit environment: Environment): Unit = {
    val hieEventTopic =
      s"projects/${environment.projectName}/topics/$topicName"

    messages
      .flatMap(message => {
        import io.circe.syntax._

        message.patient.patientId match {
          case Some(patientId) =>
            val sanitizedNotes = (for { notes <- message.fullText } yield {
              // TODO: https://cityblock.kanbanize.com/ctrl_board/2/cards/3462
              val normalizedNotes =
                Normalizer.normalize(notes, Normalizer.Form.NFD)
              normalizedNotes.replaceAll("[^\\x00-\\x7F]", "")
            }).getOrElse("")
            val event = PublishablePatientHieEvent(
              patientId = patientId,
              source = message.source,
              eventDateTime = message.eventDateTime,
              dischargeDateTime = message.dischargeDateTime,
              notes = Some(sanitizedNotes),
              locationName = message.locationName,
              fullLocationName = message.fullLocationName,
              eventType = message.eventType,
              visitType = message.visitType,
              diagnoses = message.diagnoses,
              procedures = List.empty,
              facilityType = message.facilityType,
              admissionSource = message.admissionSource,
              locationAddress = None,
              locationCity = None,
              locationZip = None,
              locationState = None,
              provider = message.provider,
              messageId = Some(message.messageId)
            )
            val payload = event.asJson.noSpaces
            val hmac = ResultSigner.signResult(payload)

            Seq((payload, Map("topic" -> "hieEvent", "hmac" -> hmac)))
          case None => Seq()
        }
      })
      .withName("publishHieEventMessages")
      .saveAsPubsubWithAttributes[String](hieEventTopic)
  }
}
