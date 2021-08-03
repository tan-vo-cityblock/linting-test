package cityblock.streaming.jobs.scheduling

import io.circe._

object AppointmentInfo {
  case class ParsedAppointmentInfo(
    code: Option[String],
    codeset: Option[String],
    description: Option[String],
    value: Option[String]
  )

  object ParsedAppointmentInfo {
    implicit val decode: Decoder[ParsedAppointmentInfo] =
      new Decoder[ParsedAppointmentInfo] {
        final def apply(cursor: HCursor): Decoder.Result[ParsedAppointmentInfo] =
          for {
            code <- cursor.downField("Code").as[Option[String]]
            codeset <- cursor.downField("Codeset").as[Option[String]]
            description <- cursor.downField("Description").as[Option[String]]
            value <- cursor.downField("Value").as[Option[String]]
          } yield {
            ParsedAppointmentInfo(code, codeset, description, value)
          }
      }
  }
}
