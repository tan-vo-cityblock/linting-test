package cityblock.ehrmodels.elation.datamodelapi.ccda

import java.util.Base64

import cityblock.ehrmodels.elation.service.Elation
import cityblock.ehrmodels.elation.service.auth.AuthData
import cityblock.utilities.backend.{ApiError, CirceError}
import com.softwaremill.sttp._
import io.circe.generic.JsonCodec
import io.circe.parser.decode

/**
 * @param id           Unique ID of the patient for which CCDA belongs.
 * @param base64_ccda  Patient chart exported as CCDA as base64-encoded string.
 */
@JsonCodec
case class Ccda(id: Long, base64_ccda: String) {
  def ccdaXml: String =
    Ccda.getCcdaXml(base64_ccda)

  def medsQueryStr: String = Ccda.getMedsQueryStr
}

object Ccda {
  def findByPatientId(
    id: String
  )(implicit backend: SttpBackend[Id, Nothing]): Either[ApiError, Ccda] =
    Elation.get[Ccda](
      uri"https://${AuthData.endpoint}/api/2.0/ccda/$id/"
    )

  def apply(ccdaJsonString: String): Either[CirceError, Ccda] =
    decode[Ccda](ccdaJsonString) match {
      case Right(ccda) => Right(ccda)
      case Left(error) => Left(CirceError(ccdaJsonString, error))
    }

  def getCcdaXml(base64Ccda: String): String =
    new String(Base64.getDecoder.decode(base64Ccda))

  def getMedsQueryStr: String =
    """
      |/ClinicalDocument
      |//component
      |//section[templateId[@root="2.16.840.1.113883.10.20.22.2.1.1"]]
      |//substanceAdministration[.//manufacturedMaterial[code[@code]]]
      |""".stripMargin
}
