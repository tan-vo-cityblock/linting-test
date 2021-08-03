package cityblock.ehrmodels.elation.datamodelapi
package physician

import com.softwaremill.sttp._
import cityblock.ehrmodels.elation.service.Elation
import cityblock.ehrmodels.elation.service.auth.AuthData
import cityblock.utilities.backend.ApiError
import io.circe.generic.JsonCodec

@JsonCodec
case class Physician(
  id: Long,
  first_name: String,
  last_name: String,
  npi: Option[String],
  specialty: Option[String],
  user_id: Int
)

object Physician {
  val baseUrl = s"https://${AuthData.endpoint}/api/2.0/physicians/"

  def findAll(implicit backend: SttpBackend[Id, Nothing]): Either[ApiError, List[Physician]] =
    Elation.getAll[Physician](uri"$baseUrl?limit=$requestLimit")

  def findPhysician(
    id: Long
  )(implicit backend: SttpBackend[Id, Nothing]): Either[ApiError, Physician] =
    Elation.get[Physician](uri"$baseUrl$id/")
}
