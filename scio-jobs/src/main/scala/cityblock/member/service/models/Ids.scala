package cityblock.member.service.models
import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveDecoder, deriveEncoder}

case class Ids(id: String, current: Option[Boolean] = None)

object Ids {
  implicit val defaultsConfig: Configuration = Configuration.default.withDefaults
  implicit val idsDecoder: Decoder[Ids] = deriveDecoder[Ids]
  implicit val IdsEncoder: Encoder[Ids] = deriveEncoder[Ids]
}
