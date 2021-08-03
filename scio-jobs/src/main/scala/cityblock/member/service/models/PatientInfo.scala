package cityblock.member.service.models

import com.spotify.scio.bigquery.types.BigQueryType
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

object PatientInfo {
  @BigQueryType.toTable
  case class Datasource(name: String, commonId: Option[String] = None)

  object Datasource {
    implicit val datasourceEncoder: Encoder[Datasource] =
      deriveEncoder[Datasource]
  }

  @BigQueryType.toTable
  case class Patient(patientId: Option[String], externalId: String, source: Datasource)

  object Patient {
    implicit val patientEncoder: Encoder[Patient] =
      deriveEncoder[Patient]
  }
}
