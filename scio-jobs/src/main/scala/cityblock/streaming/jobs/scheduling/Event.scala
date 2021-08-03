package cityblock.streaming.jobs.scheduling

import cityblock.streaming.jobs.scheduling.AppointmentInfo.ParsedAppointmentInfo
import cityblock.streaming.jobs.scheduling.Provider.ParsedProvider
import cityblock.streaming.jobs.scheduling.Diagnosis.ParsedDiagnosis
import io.circe._

object Event {
  case class ParsedEvent(
    eventType: String,
    transmissionId: Long,
    visitId: String,
    dateTime: String,
    duration: Int,
    status: Option[String],
    reason: Option[String],
    cancelReason: Option[String],
    instructions: Option[List[String]],
    facility: Option[String],
    facilityType: Option[String],
    facilityDepartment: Option[String],
    facilityRoom: Option[String],
    provider: ParsedProvider,
    attendingProvider: ParsedProvider,
    consultingProvider: Option[ParsedProvider],
    referringProvider: Option[ParsedProvider],
    diagnoses: List[ParsedDiagnosis],
    providerSchedulingNotes: List[String]
  )

  object ParsedEvent {
    implicit val decode: Decoder[ParsedEvent] = new Decoder[ParsedEvent] {
      def instructionsFilter(j: Json): Json = j.withArray { instruction =>
        Json.fromValues(instruction.filter(_.isString))
      }

      // scalastyle:off method.length
      final def apply(cursor: HCursor): Decoder.Result[ParsedEvent] =
        for {
          eventType <- cursor
            .downField("Meta")
            .downField("EventType")
            .as[String]
          transmissionId <- cursor
            .downField("Meta")
            .downField("Transmission")
            .downField("ID")
            .as[Long]
          visitId <- cursor
            .downField("Visit")
            .downField("VisitNumber")
            .as[String]
          dateTime <- cursor
            .downField("Visit")
            .downField("VisitDateTime")
            .as[String]
          duration <- cursor.downField("Visit").downField("Duration").as[Int]
          status <- cursor
            .downField("Visit")
            .downField("Status")
            .as[Option[String]]
          reason <- cursor
            .downField("Visit")
            .downField("Reason")
            .as[Option[String]]
          cancelReason <- cursor
            .downField("Visit")
            .downField("CancelReason")
            .as[Option[String]]
          instructions <- cursor
            .downField("Visit")
            .downField("Instructions")
            .withFocus(instructionsFilter)
            .as[Option[List[String]]]
          facility <- cursor
            .downField("Visit")
            .downField("Location")
            .downField("Facility")
            .as[Option[String]]
          facilityType <- cursor
            .downField("Visit")
            .downField("Location")
            .downField("Type")
            .as[Option[String]]
          facilityDepartment <- cursor
            .downField("Visit")
            .downField("Location")
            .downField("Department")
            .as[String]
          facilityRoom <- cursor
            .downField("Visit")
            .downField("Location")
            .downField("Room")
            .as[Option[String]]
          provider <- cursor
            .downField("Visit")
            .downField("VisitProvider")
            .as[ParsedProvider]
          attendingProvider <- cursor
            .downField("Visit")
            .downField("AttendingProvider")
            .as[ParsedProvider]
          consultingProvider <- cursor
            .downField("Visit")
            .downField("ConsultingProvider")
            .as[ParsedProvider]
          referringProvider <- cursor
            .downField("Visit")
            .downField("ReferringProvider")
            .as[ParsedProvider]
          diagnoses <- cursor
            .downField("Visit")
            .downField("Diagnoses")
            .as[List[ParsedDiagnosis]]
          appointmentInfos <- cursor
            .downField("AppointmentInfo")
            .as[List[ParsedAppointmentInfo]]
        } yield {
          val providerSchedulingNotes = appointmentInfos
            .withFilter(_.description.getOrElse("") != "")
            .map(schedulingNote => schedulingNote.description.getOrElse(""))

          ParsedEvent(
            eventType,
            transmissionId,
            visitId,
            dateTime,
            duration,
            status,
            reason,
            cancelReason,
            instructions,
            facility,
            facilityType,
            Some(facilityDepartment),
            facilityRoom,
            provider,
            attendingProvider,
            Some(consultingProvider),
            Some(referringProvider),
            diagnoses,
            providerSchedulingNotes
          )
        }
    }
  }
}
