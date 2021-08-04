package cityblock.ehrmodels.elation.datamodelapi.bill

import io.circe.generic.JsonCodec

@JsonCodec
case class Cpt(
  cpt: String,
  modifiers: List[String],
  dxs: List[String],
  alt_dxs: List[String],
  unit_charge: Option[String],
  units: Option[String]
)
