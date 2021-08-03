package cityblock.aggregators.models

import com.spotify.scio.bigquery.types.BigQueryType
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

object PatientEligibilities {
  @BigQueryType.toTable
  case class PatientEligibilities(
    id: Option[String],
    healthHomeState: Option[String],
    healthHomeName: Option[String],
    careManagementState: Option[String],
    careManagementName: Option[String],
    harpState: Option[String],
    harpStateDescription: Option[String],
    harpEligibility: Option[String],
    hivSnpHarpState: Option[String],
    hcbsState: Option[String]
  )
  implicit val eligibilitiesEncoder: Encoder[PatientEligibilities] =
    deriveEncoder[PatientEligibilities]
}
