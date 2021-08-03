package cityblock.transforms.cardinal.gold

import cityblock.models.CardinalSilverClaims.SilverProvider
import cityblock.models.Surrogate
import cityblock.models.cardinal.silver.Provider.ParsedProvider
import cityblock.models.gold.Claims.EntityType
import cityblock.models.gold.NewProvider._
import cityblock.models.gold.enums.TaxonomyTier
import cityblock.models.gold.{Address, Date}
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
      .withName("Construct providers")
      .map {
        case (surrogate, silver) =>
          ProviderTransformer.mkProvider(surrogate, silver)
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
      specialties = mkSpecialties(silver.data),
      taxonomies = mkTaxonomies(silver.data),
      pcpFlag = None, // TODO have asked for mapping specialty codes
      npi = silver.data.NPI,
      name = silver.data.ProviderName,
      affiliatedGroupName = silver.data.HospitalAffiliation,
      entityType = mkEntityType(silver.data),
      inNetworkFlag = mkInNetworkFlag(silver.data),
      locations = mkLocations(silver.data)
    )

  private def mkProviderIdentifier(surrogate: Surrogate,
                                   providerIdField: String): ProviderIdentifier =
    ProviderIdentifier(
      surrogate = surrogate,
      partnerProviderId = providerIdField,
      partner = partner,
      id = mkProviderId(providerIdField)
    )

  private def mkSpecialties(data: ParsedProvider): List[Specialty] =
    data.SpecialityCode.map(code => Specialty(code, Some(partner), None)).toList

  private def mkTaxonomies(data: ParsedProvider): List[Taxonomy] =
    data.TaxonomyCode.map(code => Taxonomy(code, Some(TaxonomyTier.One.name))).toList

  private def mkEntityType(data: ParsedProvider): Option[String] =
    if (data.EntityTypeCode.contains("IND")) {
      Some(EntityType.individual.toString)
    } else {
      Some(EntityType.group.toString)
    }

  private def mkInNetworkFlag(data: ParsedProvider): Option[Boolean] =
    if (data.ContractStatusCode.contains("CONT")) {
      Some(true)
    } else {
      Some(false)
    }

  private def mkLocations(data: ParsedProvider): List[Location] = {
    val clinicLocation = Address(
      address1 = data.Address1,
      address2 = data.Address2,
      city = data.City,
      state = data.State,
      county = data.County,
      zip = data.ZipCode,
      country = None,
      email = None,
      phone = data.FacilityPhone
    )

    val mailingLocation = Address(
      address1 = data.MailAddress1,
      address2 = data.MailAddress2,
      city = data.MailCity,
      state = data.MailState,
      county = data.MailCounty,
      zip = data.MailZipCode,
      country = None,
      email = None,
      phone = None
    )

    List(Location(Some(clinicLocation), Some(mailingLocation)))
  }

}
