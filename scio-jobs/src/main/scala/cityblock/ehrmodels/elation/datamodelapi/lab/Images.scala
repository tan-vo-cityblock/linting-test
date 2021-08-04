package cityblock.ehrmodels.elation.datamodelapi.lab

import io.circe.generic.JsonCodec

@JsonCodec
case class Image(document: Option[String])
