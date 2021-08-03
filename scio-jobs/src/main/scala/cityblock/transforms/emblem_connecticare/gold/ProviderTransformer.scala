package cityblock.transforms.emblem_connecticare.gold

import java.util.UUID

import cityblock.models.EmblemConnecticareSilverClaims.SilverProvider
import cityblock.models.emblem_connecticare.silver.Provider.ParsedProvider
import cityblock.models.Surrogate
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

    val providers: SCollection[SilverProvider] =
      Transform.fetchFromBigQuery[SilverProvider](
        sc,
        bigqueryProject,
        silverDataset,
        silverTable,
        deliveryDate
      )

    val goldProviders = providers
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
          ProviderTransformer.provider(surrogate, silver, deliveryDate)
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

  private def provider(surrogate: Surrogate,
                       silver: SilverProvider,
                       deliveryDate: LocalDate): Provider =
    Provider(
      providerIdentifier = mkIdentifier(surrogate, silver.data),
      dateEffective = mkDate(silver.data, deliveryDate),
      specialties = mkSpecialties(silver.data),
      taxonomies = mkTaxonomies(silver.data),
      pcpFlag = mkPcpFlag(silver.data),
      npi = silver.data.NATL_PRVD_ID,
      name = mkName(silver.data),
      affiliatedGroupName = None,
      entityType = mkEntityType(silver.data),
      inNetworkFlag = mkInNetworkFlag(silver.data),
      locations = mkLocations(silver.data)
    )

  private def mkIdentifier(surrogate: Surrogate, provider: ParsedProvider): ProviderIdentifier = {
    val providerIdField: String = provider.FACETS_PRPR_ID
    ProviderIdentifier(
      surrogate = surrogate,
      partnerProviderId = providerIdField,
      partner = "emblem",
      id = UUID.nameUUIDFromBytes(providerIdField.getBytes()).toString
    )
  }

  private def mkDate(provider: ParsedProvider, deliveryDate: LocalDate): Date = {
    val givenEndDate: Option[LocalDate] = provider.TERM_DT
    val endDate: Option[LocalDate] =
      givenEndDate.flatMap(end => if (end.isBefore(deliveryDate)) Some(end) else None)
    Date(provider.EFFV_DT, endDate)
  }

  private def mkSpecialties(provider: ParsedProvider): List[Specialty] =
    provider.PRIMARY_SPECIALTY_CODE
      .map(code => Specialty(code, Some("emblem"), None))
      .toList

  private def mkTaxonomies(provider: ParsedProvider): List[Taxonomy] =
    provider.TXNMY_CD.map(t => Taxonomy(t, Some(TaxonomyTier.One.name))).toList

  private def mkPcpFlag(provider: ParsedProvider): Option[Boolean] = {
    val providerSpecialty: Option[String] = provider.PRIMARY_SPECIALTY_CODE
    val pcpSpecialtyCodes: Set[String] = Set("IM", "FP")

    if (providerSpecialty.isDefined) {
      val isPcp: Boolean = providerSpecialty.exists(pcpSpecialtyCodes.contains)
      Some(isPcp)
    } else { None }
  }

  private def mkName(provider: ParsedProvider): Option[String] =
    Some(List(provider.FRST_NM, provider.MIDL_NM, provider.LST_NM).flatten.mkString(" "))

  private def mkEntityType(provider: ParsedProvider): Option[String] =
    if (provider.ORG_TYP_CD.isEmpty) {
      Some(EntityType.individual.toString)
    } else {
      Some(EntityType.group.toString)
    }

  private def mkInNetworkFlag(provider: ParsedProvider): Option[Boolean] = {
    val providerStatusCode: Option[String] = provider.PRV_STATUS_CD
    val inNetworkCodes: Set[String] = Set("Active Provider", "IN", "A")

    if (providerStatusCode.isDefined) {
      val isInNetwork: Boolean = providerStatusCode.exists(inNetworkCodes.contains)
      Some(isInNetwork)
    } else { None }
  }

  private def mkLocations(provider: ParsedProvider): List[Location] = {
    val clinicLocation = Address(
      address1 = provider.SVC_ADR_LN_1,
      address2 = provider.SVC_ADR_LN_2,
      city = provider.SVC_CITY,
      state = provider.SVC_ST_CD,
      county = provider.SVC_CNTY,
      zip = provider.SVC_ZIP_CD,
      country = provider.SVC_CTRY_CD,
      email = None,
      phone = provider.SVC_PRIM_WRK_PHON_NBR
    )

    val mailingLocation = Address(
      address1 = provider.BILLING_ADR_LN_1,
      address2 = provider.BILLING_ADR_LN_2,
      city = provider.BILLING_ADR_CITY,
      state = provider.BILLING_ST_CD,
      county = provider.BILLING_CNTY,
      zip = None,
      country = provider.BILLING_CTRY_CD,
      email = None,
      phone = provider.BILLING_PHON_NBR
    )

    List(Location(Some(clinicLocation), Some(mailingLocation)))
  }

}
