package cityblock.streaming.jobs.aggregations

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.utilities.time.DateOrInstant
import com.spotify.scio.bigquery.types.BigQueryType
import io.circe.Encoder
import io.circe.generic.semiauto._

object VitalSigns {
  @BigQueryType.toTable
  case class VitalSign(
    Code: String,
    CodeSystem: String,
    CodeSystemName: String,
    Name: String,
    Status: String,
    Interpretation: String,
    DateTime: Option[DateOrInstant],
    Value: String,
    Units: String
  )

  object VitalSign {
    implicit val vitalSignEncoder: Encoder[VitalSign] = deriveEncoder[VitalSign]
  }

  @BigQueryType.toTable
  case class PatientVitalSign(
    messageId: String,
    patient: Patient,
    vitalSign: VitalSign
  )

  object PatientVitalSign {
    implicit val patientVitalSignEncoder: Encoder[PatientVitalSign] =
      deriveEncoder[PatientVitalSign]
  }
}
