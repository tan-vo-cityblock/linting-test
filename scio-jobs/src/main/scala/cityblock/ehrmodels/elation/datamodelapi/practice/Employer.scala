package cityblock.ehrmodels.elation.datamodelapi.practice

import io.circe.generic.JsonCodec

@JsonCodec
case class Employer(code: String, name: Option[String], description: Option[String])
