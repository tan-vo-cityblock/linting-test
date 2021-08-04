package cityblock.ehrmodels.elation.datamodelapi.patient

import io.circe.generic.JsonCodec

@JsonCodec
case class Address(
  address_line1: Option[String],
  address_line2: Option[String],
  city: Option[String],
  state: Option[String],
  zip: Option[String]
)
