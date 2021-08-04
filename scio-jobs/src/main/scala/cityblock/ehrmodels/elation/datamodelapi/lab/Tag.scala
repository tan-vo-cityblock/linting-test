package cityblock.ehrmodels.elation.datamodelapi.lab

import io.circe.generic.JsonCodec

@JsonCodec
case class Tag(value: Option[String], code: Option[String], code_type: Option[String])
