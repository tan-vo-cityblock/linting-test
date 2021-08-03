package cityblock.models.connecticare.silver

import com.spotify.scio.bigquery.types.BigQueryType

object ProviderAddress {
  @BigQueryType.toTable
  case class ParsedProviderAddress(
    ProvNum: String,
    Address1: Option[String],
    Address2: Option[String],
    ZipCd: Option[String],
    City: Option[String],
    State: Option[String],
    County: Option[String]
  )
}
