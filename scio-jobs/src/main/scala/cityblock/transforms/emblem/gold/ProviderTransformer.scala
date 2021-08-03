package cityblock.transforms.emblem.gold

import cityblock.models.EmblemSilverClaims.SilverProvider
import cityblock.models.Surrogate
import cityblock.models.gold.Claims.EntityType
import cityblock.models.gold.NewProvider._
import cityblock.models.gold.enums.TaxonomyTier
import cityblock.models.gold.{Address, Date}
import cityblock.parsers.emblem.Provider.ParsedProvider
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
          _.identifier.surrogateId)

      Provider(
        providerIdentifier = mkIdentifier(surrogate, silver.provider),
        dateEffective = mkDate(silver.provider, deliveryDate),
        specialties = mkSpecialties(silver.provider),
        taxonomies = mkTaxonomies(silver.provider),
        pcpFlag = mkPcpFlag(silver.provider),
        npi = silver.provider.PROV_NPI,
        name = mkName(silver.provider),
        affiliatedGroupName = None,
        entityType = mkEntityType(silver.provider),
        inNetworkFlag = mkINNFlag(silver.provider),
        locations = mkLocations(silver.provider)
      )
    }

  private def mkIdentifier(surrogate: Surrogate, provider: ParsedProvider): ProviderIdentifier =
    ProviderIdentifier(
      surrogate = surrogate,
      partnerProviderId = provider.PROV_ID,
      partner = partner,
      id = ProviderKey(provider.PROV_ID, provider.PROV_LOC).uuid.toString
    )

  private def mkDate(provider: ParsedProvider, deliveryDate: LocalDate): Date = {
    val givenEndDate: Option[LocalDate] = provider.PROV_END_DATE
    val endDate: Option[LocalDate] =
      givenEndDate.flatMap(end => if (end.isBefore(deliveryDate)) Some(end) else None)
    Date(provider.PROV_START_DATE, endDate)
  }

  private def mkSpecialties(provider: ParsedProvider): List[Specialty] =
    provider.PROV_SPEC_CODE
      .map(code => Specialty(code, Some("medicare"), None))
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
    if (provider.PROV_FNAME.isEmpty || provider.PROV_FNAME.exists(Set("INC", "PC").contains)) {
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
      state = provider.PROV_CLINIC_CITY,
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
      phone = provider.PROVMAILPHONE
    )

    List(Location(Some(clinicLocation), Some(mailingLocation)))
  }
}
