package cityblock.ehrmodels.elation.datamodelapi.bill

import io.circe.generic.JsonCodec

@JsonCodec
case class ReferringProvider(name: Option[String], state: Option[String])
