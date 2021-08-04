package cityblock.ehrmodels.elation.datamodelapi.patient

import io.circe.generic.JsonCodec

@JsonCodec
case class EmergencyContact(
  first_name: Option[String],
  last_name: Option[String],
  relationship: Option[String],
  phone: Option[String],
  address_line1: Option[String],
  address_line2: Option[String],
  city: Option[String],
  state: Option[String],
  zip: Option[String]
)
