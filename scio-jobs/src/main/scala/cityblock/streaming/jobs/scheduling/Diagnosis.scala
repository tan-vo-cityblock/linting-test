package cityblock.streaming.jobs.scheduling

import io.circe._

object Diagnosis {
  case class ParsedDiagnosis(
    code: Option[String],
    codeset: Option[String],
    name: Option[String],
    diagnosisType: Option[String]
  )

  object ParsedDiagnosis {
    implicit val decode: Decoder[ParsedDiagnosis] =
      new Decoder[ParsedDiagnosis] {
        final def apply(cursor: HCursor): Decoder.Result[ParsedDiagnosis] =
          for {
            code <- cursor.downField("Code").as[Option[String]]
            codeset <- cursor.downField("Codeset").as[Option[String]]
            name <- cursor.downField("Name").as[Option[String]]
            diagnosisType <- cursor.downField("Type").as[Option[String]]
          } yield {
            ParsedDiagnosis(code, codeset, name, diagnosisType)
          }
      }
  }
}
