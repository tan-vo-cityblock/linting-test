package cityblock.ehrmodels.elation.datamodelapi
package appointment

import java.time.Instant

import cityblock.utilities.backend.CirceError

/**
 *
 * @param id unique identifier for this datamodel
 * @param scheduled_date the date for which the appointment is scheduled
 * @param duration this field is provided in minutes
 * @param reason Custom drop down menu which describes the reason for an Appointment Schedule
 * @param description Meant to support reasons for visit. Can be intended for other use cases.
 * @param status
 * @param patient the unique patient identifier in Elation.
 * @param physician the elation physician Id
 * @param practice
 * @param metadata
 * @param created_date read only
 * @param last_modified_date read only
 * @param deleted_date empty unless the object is deleted
 */
case class Appointment(
  id: Long,
  scheduled_date: Instant,
  duration: Int,
  reason: Option[String],
  description: Option[String],
  status: Option[Status],
  patient: Option[Long],
  physician: Long,
  practice: Long,
  metadata: Option[Map[String, String]],
  created_date: Option[Instant],
  last_modified_date: Option[Instant],
  deleted_date: Option[Instant]
) extends SubscriptionObject {
  override def getPatientId: Option[String] =
    this.patient.map(_.toString)

  /**
   * @return An appointment is "Cancelled" if either the Appointment is deleted or the Status is marked as "Cancelled"
   */
  def isCancelled: Boolean = {
    val cancelled =
      this.status.flatMap(_.status) match {
        case Some("Cancelled") | Some("cancelled") => true
        case _                                     => false
      }

    val deleted = this.deleted_date.nonEmpty

    deleted || cancelled
  }
}

object Appointment {
  import AppointmentImplicitDecoder._

  def apply(appointmentJson: io.circe.Json): Either[CirceError, Appointment] =
    appointmentJson.as[Appointment] match {
      case Right(elationAppointment) => Right(elationAppointment)
      case Left(error)               => Left(CirceError(appointmentJson.toString(), error))
    }
}
