package cityblock.ehrmodels.elation.datamodelapi.bill

import io.circe.generic.JsonCodec

@JsonCodec
case class ServiceLocation(
  id: Long,
  name: Option[String],
  is_primary: Option[Boolean],
  place_of_service: Option[String],
  address_line1: Option[String],
  address_line2: Option[String],
  city: Option[String],
  state: Option[String],
  zip: Option[String],
  phone: Option[String]
)
