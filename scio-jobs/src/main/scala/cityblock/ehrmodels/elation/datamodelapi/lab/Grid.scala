package cityblock.ehrmodels.elation.datamodelapi.lab

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

case class Grid(
  accession_number: Option[String],
  resulted_date: Option[LocalDateTime],
  collected_date: Option[LocalDateTime],
  status: String,
  note: Option[String],
  results: List[Result]
)

object Grid {
  implicit val Gridiso8601Decoder: Decoder[LocalDateTime] =
    io.circe.Decoder
      .decodeLocalDateTimeWithFormatter(DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ssX"))

  implicit val GridDecoder: Decoder[Grid] = deriveDecoder[Grid]
}
