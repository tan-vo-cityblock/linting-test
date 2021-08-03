package cityblock.member.service.api

import io.circe.generic.JsonCodec

@JsonCodec
case class Insurance(
  carrier: String,
  plans: List[Plan]
)

@JsonCodec
case class Plan(
  externalId: String,
  rank: Option[String],
  current: Option[Boolean],
  details: List[Detail]
)

@JsonCodec
case class Detail(
  lineOfBusiness: Option[String],
  subLineOfBusiness: Option[String],
  spanDateStart: Option[String],
  spanDateEnd: Option[String],
)
