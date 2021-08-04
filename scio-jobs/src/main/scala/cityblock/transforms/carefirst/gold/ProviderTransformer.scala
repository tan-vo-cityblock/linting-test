package cityblock.transforms.carefirst.gold

import cityblock.models.CarefirstSilverClaims.SilverProvider
import cityblock.models.Surrogate
import cityblock.models.carefirst.silver.Provider.ParsedProvider
import cityblock.models.gold.Claims.EntityType
import cityblock.models.gold.{Address, Date}
import cityblock.models.gold.NewProvider.{
  Location,
  Provider,
  ProviderIdentifier,
  Specialty,
  Taxonomy
}
import cityblock.models.gold.enums.TaxonomyTier
import cityblock.transforms.Transform
import cityblock.transforms.Transform.Transformer
import com.spotify.scio.values.SCollection
import org.joda.time.LocalDate

case class ProviderTransformer(providers: SCollection[SilverProvider], deliveryDate: LocalDate)
    extends Transformer[Provider] {
  override def transform()(implicit pr: String): SCollection[Provider] =
    ProviderTransformer.mkProvider(providers, pr, deliveryDate)
}

object ProviderTransformer {
  def mkProvider(silverProviders: SCollection[SilverProvider],
                 project: String,
                 deliveryDate: LocalDate): SCollection[Provider] =
    silverProviders.map { silver =>
      val (surrogate, _) =
        Transform.addSurrogate(project, "silver_claims", "Provider", silver)(
          _.identifier.surrogateId
        )

      Provider(
        providerIdentifier = mkIdentifier(surrogate, silver.data),
        dateEffective = mkDate(silver.data, deliveryDate),
        specialties = mkSpecialties(silver.data),
        taxonomies = mkTaxonomies(silver.data),
        pcpFlag = mkPcpFlag(silver.data),
        npi = silver.data.PROV_NPI,
        name = mkName(silver.data),
        affiliatedGroupName = None, // TODO this is none in Emblem
        entityType = mkEntityType(silver.data),
        inNetworkFlag = mkINNFlag(silver.data),
        locations = mkLocations(silver.data)
      )
    }

  private def mkIdentifier(surrogate: Surrogate, provider: ParsedProvider): ProviderIdentifier =
    ProviderIdentifier(
      surrogate = surrogate,
      partnerProviderId = provider.PROV_ID,
      partner = partner,
      id = ProviderKey(provider.PROV_ID).uuid.toString
    )

  private def mkDate(provider: ParsedProvider, deliveryDate: LocalDate): Date = {
    val givenEndDate: Option[LocalDate] = provider.PROV_EFF_TO_DATE
    val endDate: Option[LocalDate] =
      givenEndDate.flatMap(end => if (end.isBefore(deliveryDate)) Some(end) else None)
    Date(provider.PROV_EFF_FROM_DATE, endDate)
  }

  private def mkSpecialties(provider: ParsedProvider): List[Specialty] =
    provider.PROV_SPEC_CODE
      .map(code => Specialty(code, Some("medicare"), None)) // TODO verify "medicare"
      .toList

  private def mkTaxonomies(provider: ParsedProvider): List[Taxonomy] =
    provider.PROV_TAXONOMY.map(t => Taxonomy(t, Some(TaxonomyTier.One.name))).toList

  private def mkPcpFlag(provider: ParsedProvider): Option[Boolean] = {
    val providerSpecialty: Option[String] = provider.PROV_SPEC_CODE
    val pcpSpecialtyCodes: Set[String] = Set("01", "08", "11", "37", "38", "84")

    if (providerSpecialty.isDefined) {
      val isPcp: Boolean = providerSpecialty.exists(pcpSpecialtyCodes.contains)
      Some(isPcp)
    } else { None }
  }

  private def mkName(provider: ParsedProvider): Option[String] = {
    val providerName =
      List(provider.PROV_FNAME, provider.PROV_MNAME, provider.PROV_LNAME).flatten.mkString(" ")

    if (providerName.isEmpty) {
      provider.PROV_GRP_NAME
    } else {
      Some(providerName)
    }
  }

  private def mkEntityType(provider: ParsedProvider): Option[String] =
    if (provider.PROV_FNAME.isEmpty) {
      Some(EntityType.group.toString)
    } else {
      Some(EntityType.individual.toString)
    }

  private def mkINNFlag(provider: ParsedProvider): Option[Boolean] =
    provider.PROV_PAR_FLAG.map {
      case "Y" => true
      case _   => false
    }

  private def mkLocations(provider: ParsedProvider): List[Location] = {
    val clinicLocation = Address(
      address1 = provider.PROV_CLINIC_ADDR,
      address2 = provider.PROV_CLINIC_ADDR2,
      city = provider.PROV_CLINIC_CITY,
      state = provider.PROV_CLINIC_STATE,
      county = None,
      zip = provider.PROV_CLINIC_ZIP,
      country = None,
      email = provider.PROV_EMAIL,
      phone = provider.PROV_PHONE
    )

    val mailingLocation = Address(
      address1 = provider.PROV_MAILING_ADDR,
      address2 = provider.PROV_MAILING_ADDR2,
      city = provider.PROV_MAILING_CITY,
      state = provider.PROV_MAILING_STATE,
      county = None,
      zip = provider.PROV_MAILING_ZIP,
      country = None,
      email = None,
      phone = provider.PROV_GRP_PHONE
    )

    List(Location(Some(clinicLocation), Some(mailingLocation)))
  }
}
