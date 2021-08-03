package cityblock.ehrmodels.elation.datamodelapi.lab

import io.circe.generic.JsonCodec

@JsonCodec
case class TestCategory(value: Option[String], description: Option[String])
