package cityblock.member.service.api
import cityblock.member.service.models.PatientInfo.{Datasource, Patient}
import io.circe.generic.JsonCodec

@JsonCodec
case class MemberInsuranceRecord(memberId: String,
                                 externalId: String,
                                 carrier: String,
                                 current: Option[Boolean]) {
  def toPatient: Patient =
    new Patient(patientId = Some(this.memberId),
                this.externalId,
                source = Datasource(name = carrier))
}
