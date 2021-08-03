package cityblock.ehrmodels.elation.datamodelapi.patient

import io.circe.generic.JsonCodec

@JsonCodec
case class Employer(code: Option[String], name: Option[String], description: Option[String])
