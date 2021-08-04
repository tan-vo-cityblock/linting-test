package cityblock.models.gold

import cityblock.models.Surrogate
import com.spotify.scio.bigquery.types.BigQueryType

object NewProvider {
  @BigQueryType.toTable
  case class ProviderIdentifier(
    surrogate: Surrogate,
    partnerProviderId: String,
    partner: String,
    id: String
  )

  @BigQueryType.toTable
  case class Specialty(
    code: String,
    codeset: Option[String],
    tier: Option[String]
  )

  @BigQueryType.toTable
  case class Taxonomy(
    code: String,
    tier: Option[String]
  )

  @BigQueryType.toTable
  case class Location(
    clinic: Option[Address],
    mailing: Option[Address]
  )

  @BigQueryType.toTable
  case class Provider(
    providerIdentifier: ProviderIdentifier,
    dateEffective: Date,
    specialties: List[Specialty],
    taxonomies: List[Taxonomy],
    pcpFlag: Option[Boolean],
    npi: Option[String],
    name: Option[String],
    affiliatedGroupName: Option[String],
    entityType: Option[String],
    inNetworkFlag: Option[Boolean],
    locations: List[Location]
  )
}
