package cityblock.member.service.api

import io.circe.generic.JsonCodec

@JsonCodec
case class MemberCreateAndPublishResponse(
  patientId: String,
  cityblockId: String,
  mrn: Option[String],
  messageId: String
)
