package cityblock.streaming.jobs.scheduling

import io.circe._

object Provider {
  case class ParsedProvider(
    id: Option[String],
    idType: Option[String],
    credentials: Option[List[String]],
    firstName: Option[String],
    lastName: Option[String]
  )

  object ParsedProvider {
    implicit val decode: Decoder[ParsedProvider] = new Decoder[ParsedProvider] {
      final def apply(cursor: HCursor): Decoder.Result[ParsedProvider] =
        for {
          id <- cursor.downField("ID").as[Option[String]]
          idType <- cursor.downField("IDType").as[Option[String]]
          credentials <- cursor
            .downField("Credentials")
            .as[Option[List[String]]]
          firstName <- cursor.downField("FirstName").as[Option[String]]
          lastName <- cursor.downField("LastName").as[Option[String]]
        } yield {
          ParsedProvider(id, idType, credentials, firstName, lastName)
        }
    }
  }
}
