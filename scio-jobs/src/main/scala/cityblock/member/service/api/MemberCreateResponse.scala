package cityblock.member.service.api

import io.circe.generic.JsonCodec

@JsonCodec
case class MemberCreateResponse(patientId: String, cityblockId: String)
