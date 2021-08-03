package cityblock.ehrmodels.elation.datamodelapi.lab

import io.circe.generic.JsonCodec

@JsonCodec
case class Test(code: Option[String], name: String, loinc: Option[String])
