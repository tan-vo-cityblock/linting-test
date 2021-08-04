package cityblock.transforms.tufts.gold

import cityblock.models.Surrogate
import com.spotify.scio.values.SCollection
import cityblock.models.TuftsSilverClaims.SilverProvider
import cityblock.models.gold.Claims.EntityType
import cityblock.models.gold.{Address, Date}
import cityblock.models.gold.NewProvider.{
  Location,
  Provider,
  ProviderIdentifier,
  Specialty,
  Taxonomy
}
import cityblock.models.tufts.silver.Provider.ParsedProvider
import cityblock.transforms.Transform.Transformer
import cityblock.transforms.Transform
import cityblock.models.gold.enums.{SpecialtyTier, TaxonomyTier}
import cityblock.transforms.tufts.ProviderKey
import org.joda.time.LocalDate

case class ProviderTransformer(
  providers: SCollection[SilverProvider],
  deliveryDate: LocalDate
) extends Transformer[Provider] {
  override def transform()(implicit pr: String): SCollection[Provider] =
    ProviderTransformer.mkProvider(providers, pr, deliveryDate)
}

object ProviderTransformer {
  private def mkProviderId(provider: ParsedProvider): String =
    ProviderKey(provider.Plan_Provider_ID).uuid.toString

  private def mkProviderIdentifer(provider: ParsedProvider,
                                  surrogate: Surrogate): ProviderIdentifier =
    ProviderIdentifier(
      surrogate = surrogate,
      partnerProviderId = provider.Plan_Provider_ID,
      partner = partner,
      id = mkProviderId(provider)
    )

  private def mkInNetworkFlag(provider: ParsedProvider): Option[Boolean] =
    provider.PPO_Indicator.map {
      case "1" => true
      case _   => false
    }

  private def mkEntityType(provider: ParsedProvider): Option[String] =
    if (provider.Last_Name.isDefined) Some(EntityType.individual.toString)
    else Some(EntityType.group.toString)

  private def mkSpecialties(provider: ParsedProvider): List[Specialty] =
    provider.Primary_Specialty_Code
      .map(code => {
        Specialty(code, Some("medicare"), Some(SpecialtyTier.One.name))
      })
      .toList

  private def mkTaxonomies(provider: ParsedProvider): List[Taxonomy] =
    provider.Taxonomy.map(taxonomy => Taxonomy(taxonomy, Some(TaxonomyTier.One.name))).toList

  val pcpSpecialtyCodes: Set[String] = Set("01", "08", "11", "37", "38", "84")

  private def isPcpFlag(flag: Option[String]): Boolean = flag.exists(pcpSpecialtyCodes.contains)

  private def mkPcpFlag(provider: ParsedProvider): Option[Boolean] = {
    val primaryCode: Option[String] = provider.Primary_Specialty_Code
    val secondaryCode: Option[String] = provider.Secondary_Specialty2_Code
    val tertiaryCode: Option[String] = provider.Secondary_Specialty3_Code
    val quaternaryCode: Option[String] = provider.Secondary_Specialty4_Code

    if (primaryCode.isDefined || secondaryCode.isDefined || tertiaryCode.isDefined || quaternaryCode.isDefined) {
      val isPcp: Boolean = {
        isPcpFlag(primaryCode) || isPcpFlag(secondaryCode) || isPcpFlag(tertiaryCode) || isPcpFlag(
          quaternaryCode)
      }
      Some(isPcp)
    } else {
      None
    }
  }

  private def mkDate(provider: ParsedProvider, deliveryDate: LocalDate): Date = {
    val endDate: Option[LocalDate] = provider.End_Date
    val endDateToUse: Option[LocalDate] =
      endDate.flatMap(end => if (end.isBefore(deliveryDate)) Some(end) else None)
    Date(provider.Begin_Date, endDateToUse)
  }

  private def mkName(provider: ParsedProvider): Option[String] = {
    val providerName =
      List(provider.First_Name, provider.Middle_Initial, provider.Last_Name).flatten.mkString(" ")

    if (providerName.isEmpty) {
      provider.Entity_Name
    } else {
      Some(providerName)
    }
  }

  private def mkGroupName(provider: ParsedProvider): Option[String] = {
    val entityName = provider.Entity_Name
    if (entityName.isEmpty) {
      provider.Provider_Affiliation.orElse(Some(provider.Plan_Provider_ID))
    } else {
      entityName
    }
  }

  private def mkLocations(provider: ParsedProvider): List[Location] = {
    val clinicLocation = Address(
      address1 = provider.Street_Address1_Name,
      address2 = provider.Street_Address2_Name,
      city = provider.City_Name,
      state = provider.State_Code,
      county = None,
      zip = provider.Zip_Code,
      country = provider.Country_Code,
      email = None,
      phone = provider.Provider_Telephone
    )

    val mailingLocation = Address(
      address1 = provider.Mailing_Street_Address1_Name,
      address2 = provider.Mailing_Street_Address2_Name,
      city = provider.Mailing_City_Name,
      state = provider.Mailing_State_Code,
      county = None,
      zip = provider.Mailing_Zip_Code,
      country = provider.Mailing_Country_Code,
      email = None,
      phone = None
    )

    List(Location(Some(clinicLocation), Some(mailingLocation)))
  }

  def mkProvider(
    silverProviders: SCollection[SilverProvider],
    project: String,
    deliveryDate: LocalDate
  ): SCollection[Provider] =
    silverProviders.map { silverProvider =>
      val (surrogate, _) =
        Transform.addSurrogate(project, "silver_claims", "Provider", silverProvider)(
          _.identifier.surrogateId)
      val provider = silverProvider.data

      Provider(
        providerIdentifier = mkProviderIdentifer(provider, surrogate),
        dateEffective = mkDate(provider, deliveryDate),
        specialties = mkSpecialties(provider),
        taxonomies = mkTaxonomies(provider),
        pcpFlag = mkPcpFlag(provider),
        npi = provider.National_Provider_ID,
        name = mkName(provider),
        affiliatedGroupName = mkGroupName(provider),
        entityType = mkEntityType(provider),
        inNetworkFlag = mkInNetworkFlag(provider),
        locations = mkLocations(provider)
      )
    }
}
