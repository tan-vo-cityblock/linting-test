package cityblock.models.healthyblue.silver

import com.spotify.scio.bigquery.types.BigQueryType

object Provider {
  @BigQueryType.toTable
  case class ParsedProvider(
    NPI: Option[String],
    NPIEntityType: Option[String],
    FullName: Option[String],
    FirstName: Option[String],
    LastName: Option[String],
    TaxonomyCode: Option[String],
    NPIPhysicalAddress1: Option[String],
    NPIPhysicalAddress2: Option[String],
    NPIPhysicalCity: Option[String],
    NPIPhysicalState: Option[String],
    NPIPhysicalPostalCode: Option[String],
    Zip_Code: Option[String]
  )
}
