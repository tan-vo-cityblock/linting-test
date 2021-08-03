package cityblock.ehrmodels.elation.datamodelapi
package lab

import java.time.Instant

import cityblock.ehrmodels.elation.service.Elation
import cityblock.ehrmodels.elation.service.auth.AuthData
import cityblock.utilities.backend.{ApiError, CirceError}
import com.softwaremill.sttp._

case class Lab(
  id: Long,
  patient: Long,
  custom_title: Option[String],
  report_type: Option[String],
  requisition_number: Option[String],
  tags: List[Tag],
  reported_date: Option[Instant],
  last_collected_date: Option[Instant],
  reviewed_by: Option[String],
  grids: List[Grid],
  images: List[Image],
  files: List[String],
  practice: Long,
  physician: Long,
  document_date: Instant,
  chart_date: Option[Instant],
  signed_date: Option[Instant],
  signed_by: Option[Long],
  created_date: Instant,
  deleted_date: Option[Instant],
  printable_view: Option[String]
) extends SubscriptionObject {
  override def getPatientId: Option[String] = Some(this.patient.toString)
}

object Lab {
  import LabImplicitDecoder._

  val baseUrl = s"https://${AuthData.endpoint}/api/2.0/reports/"

  def findById(id: String)(implicit backend: SttpBackend[Id, Nothing]): Either[ApiError, Lab] =
    Elation.get[Lab](uri"$baseUrl$id")

  def apply(labJson: io.circe.Json): Either[CirceError, Lab] =
    labJson.as[Lab] match {
      case Right(elationLab) => Right(elationLab)
      case Left(error)       => Left(CirceError(labJson.toString(), error))
    }
}
