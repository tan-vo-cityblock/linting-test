package cityblock.ehrmodels.elation.datamodelapi.lab

import io.circe.generic.JsonCodec

@JsonCodec
case class File(content_type: Option[String], external_content: Option[ExternalContent])

@JsonCodec
case class ExternalContent(referenced_url: Option[String], summary: Option[String])
