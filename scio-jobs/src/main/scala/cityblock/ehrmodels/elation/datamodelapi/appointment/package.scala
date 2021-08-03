package cityblock.ehrmodels.elation.datamodelapi

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

package object appointment {
  object AppointmentImplicitDecoder {
    implicit val appointmentiso8601Decoder: Decoder[LocalDateTime] =
      io.circe.Decoder
        .decodeLocalDateTimeWithFormatter(DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ssX"))

    implicit val appointmentDecoder: Decoder[Appointment] =
      deriveDecoder[Appointment]
  }
}
