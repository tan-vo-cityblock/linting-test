package cityblock.ehrmodels.elation.datamodelapi.patient

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class Email(
  email: Option[String],
  created_date: Option[LocalDateTime],
  deleted_data: Option[LocalDateTime]
)

object Email {
  implicit val emailiso8601Decoder: Decoder[LocalDateTime] =
    io.circe.Decoder.decodeLocalDateTimeWithFormatter(
      DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ssX")
    )

  implicit val emailDecoder: Decoder[Email] = deriveDecoder[Email]
  implicit val emailEncoder: Encoder[Email] = deriveEncoder[Email]
}
