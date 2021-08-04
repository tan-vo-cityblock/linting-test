package cityblock.ehrmodels.elation.datamodelapi

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import cityblock.ehrmodels.elation.service.auth.AuthData
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

package object lab {
  object LabImplicitDecoder {

    val baseUrl = s"https://${AuthData.endpoint}/api/2.0/reports/"

    implicit val Labiso8601Decoder: Decoder[LocalDateTime] =
      io.circe.Decoder
        .decodeLocalDateTimeWithFormatter(DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ssX"))

    implicit val LabDecoder: Decoder[Lab] = deriveDecoder[Lab]
  }
}
