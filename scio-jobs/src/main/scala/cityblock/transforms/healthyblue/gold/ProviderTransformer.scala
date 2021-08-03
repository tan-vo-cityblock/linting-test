package cityblock.transforms.healthyblue.gold

import cityblock.models.HealthyBlueSilverClaims.SilverProvider
import cityblock.models.Surrogate
import cityblock.models.gold.Claims.EntityType
import cityblock.models.gold.{Address, Date}
import cityblock.models.gold.NewProvider.{Location, Provider, ProviderIdentifier, Taxonomy}
import cityblock.models.gold.enums.TaxonomyTier
import cityblock.models.healthyblue.silver.Provider.ParsedProvider
import cityblock.transforms.Transform
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

object ProviderTransformer {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)

    val bigqueryProject: String = args.required("bigqueryProject")

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val deliveryDate: LocalDate = LocalDate.parse(args.required("deliveryDate"), dtf)

    val silverDataset = args.required("silverDataset")
    val silverTable = args.required("silverTable")

    val goldDataset = args.required("goldDataset")
    val goldTable = args.required("goldTable")

    val silverProviders: SCollection[SilverProvider] =
      Transform.fetchFromBigQuery[SilverProvider](
        sc,
        bigqueryProject,
        silverDataset,
        silverTable,
        deliveryDate
      )

    val goldProviders = silverProviders
      .withName("Add surrogates")
      .map {
        Transform.addSurrogate(
          bigqueryProject,
          silverDataset,
          silverTable,
          _
        )(_.identifier.surrogateId)
      }
      .withName("Construct gold providers")
      .map {
        case (surrogate, silver) =>
          mkProvider(surrogate, silver)
      }

    Transform.persist(
      goldProviders,
      bigqueryProject,
      goldDataset,
      goldTable,
      deliveryDate,
      WRITE_TRUNCATE,
      CREATE_IF_NEEDED
    )

    sc.close().waitUntilFinish()
  }

  private def mkProvider(surrogate: Surrogate, silver: SilverProvider): Provider =
    Provider(
      providerIdentifier = mkProviderIdentifier(surrogate, silver.data.NPI.getOrElse("")),
      dateEffective = Date(None, None),
      specialties = List.empty, // TODO use taxonomy codes to fill in specialty codes
      taxonomies = mkTaxonomy(silver.data),
      pcpFlag = None,
      npi = silver.data.NPI,
      name = silver.data.FullName,
      affiliatedGroupName = None,
      entityType = mkEntityType(silver.data),
      inNetworkFlag = Some(true),
      locations = mkLocations(silver.data)
    )

  private def mkProviderIdentifier(surrogate: Surrogate,
                                   providerIdField: String): ProviderIdentifier =
    ProviderIdentifier(
      surrogate = surrogate,
      partnerProviderId = providerIdField,
      partner = partner,
      id = Transform.mkIdentifier(providerIdField)
    )

  private def mkTaxonomy(data: ParsedProvider): List[Taxonomy] =
    data.TaxonomyCode.map(code => Taxonomy(code, Some(TaxonomyTier.One.name))).toList

  private def mkEntityType(data: ParsedProvider): Option[String] =
    if (data.NPIEntityType.contains("Clinician")) {
      Some(EntityType.individual.toString)
    } else if (data.NPIEntityType.contains("Provider")) {
      Some(EntityType.group.toString)
    } else {
      None
    }

  private def mkLocations(data: ParsedProvider): List[Location] = {
    val clinicLocation = Address(
      address1 = data.NPIPhysicalAddress1,
      address2 = data.NPIPhysicalAddress2,
      city = data.NPIPhysicalCity,
      state = data.NPIPhysicalState,
      county = None,
      zip = data.Zip_Code,
      country = None,
      email = None,
      phone = None
    )

    List(Location(Some(clinicLocation), None))
  }
}
