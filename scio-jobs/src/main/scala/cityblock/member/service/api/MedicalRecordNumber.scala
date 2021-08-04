package cityblock.member.service.api
import io.circe.generic.JsonCodec

@JsonCodec
case class MedicalRecordNumber(id: Option[String], name: Option[String])
