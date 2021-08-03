package cityblock.ehrmodels.elation.datamodelapi.practice

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

case class ServiceLocation(
  id: Long,
  name: Option[String],
  is_primary: Boolean,
  place_of_service: Option[String],
  address_line1: Option[String],
  address_line2: Option[String],
  city: Option[String],
  state: Option[String],
  zip: Option[String],
  phone: Option[String],
  fax: Option[String],
  created_date: Option[LocalDateTime],
  deleted_date: Option[LocalDateTime]
)

object ServiceLocation {
  implicit val ServiceLocationiso8601Decoder: Decoder[LocalDateTime] =
    io.circe.Decoder
      .decodeLocalDateTimeWithFormatter(DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ssX"))

  implicit val ServiceLocationDecoder: Decoder[ServiceLocation] =
    deriveDecoder[ServiceLocation]
}
