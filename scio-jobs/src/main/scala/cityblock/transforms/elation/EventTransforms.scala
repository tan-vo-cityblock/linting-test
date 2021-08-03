package cityblock.transforms.elation

import cityblock.ehrmodels.elation.datamodelapi.appointment._
import cityblock.ehrmodels.elation.datamodelapi.practice.Practice
import cityblock.streaming.jobs.ElationSubscriber.pullElationPhysician
import cityblock.streaming.jobs.scheduling.Diagnosis.ParsedDiagnosis
import cityblock.streaming.jobs.scheduling.Event.ParsedEvent
import cityblock.streaming.jobs.scheduling.Provider.ParsedProvider
import cityblock.utilities.backend.SttpHelper
import cityblock.utilities.{Loggable, ResultSigner}
import com.softwaremill.sttp.{Id, SttpBackend}
import io.circe.syntax._
import io.circe.generic.auto._

object EventTransforms extends Loggable {

  /**
   * In the appointment object, the Diagnosis is not provided. Thus when converting to a Redox object, it is passed in
   * as an empty list
   *
   * @param appt The elation appointment object that needs to be converted to a Redox Object
   * @param transmissionId Used by commons in order to determine the order for a Common's object
   * @param allElationPractices Map of [PracticeId, Practice] for all practices in elation.
   * @return A tuple of a Json String and an Attribute map in order to publish to Commons via pubsub
   */
  def appointmentToPubSubMessage(
    appt: Appointment,
    transmissionId: Long,
    patientId: String,
    allElationPractices: Map[Long, Practice],
    testing: Boolean
  ): (String, Map[String, String]) = {
    implicit val backend: SttpBackend[Id, Nothing] = SttpHelper.getBackend(testing)

    val eventType: EventMessageHeader.SchedulingEventType =
      if (appt.isCancelled) {
        EventMessageHeader.SchedulingEventType.Cancel
      } else {
        EventMessageHeader.SchedulingEventType.Reschedule
      }

    val visitId: String = appt.id.toString

    val dateTime: String = appt.scheduled_date.toString

    val duration = appt.duration

    val elationStatus = appt.status

    val status = elationStatus.flatMap(_.status)

    val facilityRoom = elationStatus.flatMap(_.room)

    val reason = appt.reason

    val cancelReason: Option[String] = None

    val provider =
      pullElationPhysician(appt.physician) match {
        case Some(physician) =>
          ParsedProvider(
            physician.npi,
            Some("NPI"),
            None,
            Some(physician.first_name),
            Some(physician.last_name)
          )
        case None =>
          logger.error(
            //          #TODO: Need to allow for better error handling/retrying
            s"Unable to find physician: ${appt.physician} with AppointmentId : $visitId in allPhysicians Map."
          )
          ParsedProvider(None, None, None, None, None)
      }

    val facility: Option[String] = allElationPractices
      .get(appt.practice)
      .map(_.name)
      .orElse {
        logger.error(
          s"Unable to find practice: ${appt.practice} with AppointmentId : $visitId in allPractices Map"
        )
        None
      }

    val providerSchedulingNotes = appt.description match {
      case Some(descr) => List[String](descr)
      case _           => List[String]()
    }

    // TODO: Expose description as a thing to Commons or otherwise improve handling of these bits of info about the appt
    val instructions: Option[List[String]] = Some(reason.toList ++ providerSchedulingNotes)

    val parsedEvent = ParsedEvent(
      eventType.toString,
      transmissionId,
      visitId,
      dateTime,
      duration,
      status,
      reason,
      cancelReason,
      instructions,
      facility,
      None,
      facility,
      facilityRoom,
      provider,
      provider,
      None,
      None,
      List[ParsedDiagnosis](), // Empty Diagnosis
      providerSchedulingNotes
    )

    val decodedEvent =
      parsedEvent.asJsonObject.add("patientId", patientId.asJson)
    val messageJson = decodedEvent.asJson.noSpaces

    val hmac = ResultSigner.signResult(messageJson)

    val redoxMap: EventMessageHeader =
      EventMessageHeader(eventType, eventType, EventMessageHeader.Topic.scheduling, hmac)

    (messageJson, redoxMap.toMap)
  }
}

// TODO: Add this to Redox
case class EventMessageHeader(
  `type`: EventMessageHeader.SchedulingEventType,
  schedulingEventType: EventMessageHeader.SchedulingEventType,
  topic: EventMessageHeader.Topic,
  hmac: String
) {
  def toMap: Map[String, String] =
    Map(
      "type" -> `type`.toString,
      "schedulingEventType" -> schedulingEventType.toString,
      "topic" -> topic.toString,
      "hmac" -> hmac
    )
}

object EventMessageHeader {
  type SchedulingEventType = SchedulingEventType._SchedulingEventType
  type Topic = Topic._Topic

  object SchedulingEventType extends Enumeration {
    type _SchedulingEventType = Value
    val Cancel, Reschedule = Value
  }

  object Topic extends Enumeration {
    type _Topic = Value
    val scheduling = Value
  }
}
