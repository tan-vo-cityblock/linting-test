package cityblock.member.service.models

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class ExternalId(datasource: String, ids: List[Ids])

object ExternalId {
  implicit val externalIdRequestDecoder: Decoder[ExternalId] = deriveDecoder[ExternalId]
  implicit val externalIdRequestEncoder: Encoder[ExternalId] = deriveEncoder[ExternalId]
}
