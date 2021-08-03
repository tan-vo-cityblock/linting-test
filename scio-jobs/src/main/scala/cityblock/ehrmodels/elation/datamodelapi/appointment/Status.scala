package cityblock.ehrmodels.elation.datamodelapi.appointment

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

case class Status(status: Option[String], room: Option[String], status_date: Option[LocalDateTime])

object Status {
  implicit val statusIso8601Decoder: Decoder[LocalDateTime] =
    Decoder.decodeLocalDateTimeWithFormatter(DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ssX"))

  implicit val statusDecoder: Decoder[Status] =
    deriveDecoder[Status]
}
