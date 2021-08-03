package cityblock.ehrmodels.elation.datamodelapi

import cityblock.ehrmodels.elation.datamodelapi.appointment.Appointment
import cityblock.ehrmodels.elation.datamodelapi.bill.Bill
import cityblock.ehrmodels.elation.datamodelapi.lab.Lab
import cityblock.ehrmodels.elation.datamodelapi.patient.Patient
import cityblock.ehrmodels.elation.datamodelapi.vital.Vital
import cityblock.utilities.backend.{ApiError, UnknownResponseError}
import io.circe.Json

/* TODO: Add an abstract function that states whether an object is soft-deleted by elation, by pattern matching
         on the deleted_date for every queryable object. This would allow for a simpler deletion ability in the future.
 */
trait SubscriptionObject {
  def getPatientId: Option[String]
}

object SubscriptionObject {
  def apply(dataJson: Json, resource: String): Either[ApiError, SubscriptionObject] =
    resource match {
      case "patients"     => Patient(dataJson)
      case "appointments" => Appointment(dataJson)
      case "vitals"       => Vital(dataJson)
      case "reports"      => Lab(dataJson)
      case "bills" =>
        Bill(dataJson) // Hooking bills event for opportunistic pulls in ElationSubscriber
      case _ =>
        Left(UnknownResponseError(s"Unable to find data model type for resource $resource"))
    }
}
