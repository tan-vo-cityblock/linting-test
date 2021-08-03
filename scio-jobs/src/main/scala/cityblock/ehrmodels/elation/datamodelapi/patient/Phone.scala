package cityblock.ehrmodels.elation.datamodelapi.patient

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class Phone(
  phone: String,
  phone_type: Enums.PhoneType.PhoneType,
  created_date: Option[LocalDateTime],
  deleted_data: Option[LocalDateTime]
)

object Phone {
  implicit val phoneiso8601Decoder: Decoder[LocalDateTime] =
    io.circe.Decoder.decodeLocalDateTimeWithFormatter(
      DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ssX")
    )

  implicit val decoder: Decoder[Phone] = deriveDecoder[Phone]
  implicit val encoder: Encoder[Phone] = deriveEncoder[Phone]
}
