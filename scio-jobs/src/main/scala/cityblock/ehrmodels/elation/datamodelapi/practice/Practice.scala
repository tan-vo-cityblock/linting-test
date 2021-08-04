package cityblock.ehrmodels.elation
package datamodelapi.practice

import cityblock.ehrmodels.elation.service.Elation
import cityblock.ehrmodels.elation.service.auth.AuthData
import cityblock.utilities.backend.ApiError
import com.softwaremill.sttp._
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

case class Practice(
  id: Long,
  name: String,
  address_line1: String,
  address_line2: Option[String],
  city: String,
  state: String,
  zip: String,
  timezone: Option[String],
  employers: List[Employer],
  physicians: List[Long],
  service_locations: List[ServiceLocation]
)

object Practice {
  implicit val patientDecoder: Decoder[Practice] = deriveDecoder[Practice]

  def findById(
    practiceId: String
  )(implicit backend: SttpBackend[Id, Nothing]): Either[ApiError, Practice] =
    Elation.get[Practice](uri"https://${AuthData.endpoint}/api/2.0/practices/$practiceId")

  def findAll(implicit backend: SttpBackend[Id, Nothing]): Either[ApiError, List[Practice]] =
    Elation.getAll[Practice](uri"https://${AuthData.endpoint}/api/2.0/practices/?limit=50")
}
