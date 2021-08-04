package cityblock.ehrmodels.elation.datamodelapi.bill

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

case class Payment(amount: Option[String], when_collected: Option[LocalDateTime])

object Payment {
  implicit val PaymentIso8601Decoder: Decoder[LocalDateTime] =
    io.circe.Decoder
      .decodeLocalDateTimeWithFormatter(DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ssX"))

  implicit val PaymentDecoder: Decoder[Payment] = deriveDecoder[Payment]
}
