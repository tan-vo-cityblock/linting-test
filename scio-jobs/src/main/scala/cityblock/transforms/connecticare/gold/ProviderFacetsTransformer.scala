package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.FacetsProvider
import cityblock.models.Surrogate
import cityblock.models.connecticare.silver.FacetsProvider.ParsedFacetsProvider
import cityblock.models.gold.Claims.EntityType
import cityblock.models.gold.NewProvider.{Location, Provider, ProviderIdentifier, Specialty}
import cityblock.models.gold.enums.SpecialtyTier
import cityblock.models.gold.{Address, Date}
import cityblock.transforms.Transform
import cityblock.transforms.Transform.Transformer
import com.spotify.scio.values.SCollection
import org.joda.time.LocalDate

case class ProviderFacetsTransformer(silverProviders: SCollection[FacetsProvider])
    extends Transformer[Provider] {
  override def transform()(implicit pr: String): SCollection[Provider] =
    ProviderFacetsTransformer.pipeline(pr, silverProviders)
}

object ProviderFacetsTransformer {
  def pipeline(project: String,
               silverProviders: SCollection[FacetsProvider]): SCollection[Provider] =
    silverProviders
      .map { claim =>
        val (surrogate, _) =
          Transform.addSurrogate(project, "silver_claims_facets", table = "FacetsProvider", claim)(
            _.identifier.surrogateId)
        mkProvider(claim, surrogate)
      }
      .withName("Remove duplicate rows")
      .distinctBy {
        _.copy(providerIdentifier = ProviderIdentifier(Surrogate("", "", "", ""), "", "", ""))
      }
      .withName("Group by id, dateEffective.from, dateEffective.to")
      .groupBy(row => (row.providerIdentifier.id, row.dateEffective.from, row.dateEffective.to))
      .values
      .withName("Aggregate location data")
      .map { rows =>
        rows.head.copy(locations = rows.flatMap(_.locations).toList)
      }

  def mkProvider(silverProvider: FacetsProvider, surrogate: Surrogate): Provider =
    Provider(
      providerIdentifier = mkIdentifier(silverProvider.data, surrogate),
      dateEffective = mkDate(silverProvider.data),
      specialties = mkSpecialties(silverProvider.data),
      taxonomies = List(),
      pcpFlag = mkPcpFlag(silverProvider.data),
      npi = silverProvider.data.NPI,
      name = mkName(silverProvider.data),
      affiliatedGroupName = silverProvider.data.Billing_Name,
      entityType = mkEntityType(silverProvider.data),
      inNetworkFlag = mkInNetworkFlag(silverProvider.data),
      locations = mkLocations(silverProvider.data)
    )

  def mkIdentifier(data: ParsedFacetsProvider, surrogate: Surrogate): ProviderIdentifier =
    ProviderIdentifier(
      surrogate = surrogate,
      partnerProviderId = data.Tru_Prov_Nbr,
      partner = partner,
      id = FacetsProviderKey(data).uuid.toString
    )

  /**
   * Business logic: CCI sends us Facets data with Prov_Elig_End_Date = "9999-12-31" for active timespans,
   * but our gold tables have toDate null.
   */
  def mkDate(data: ParsedFacetsProvider): Date = {
    val fromDate: Option[LocalDate] = data.Prov_Elig_Start_Date
    val toDate: Option[LocalDate] = data.Prov_Elig_End_Date match {
      case Some(date) => if (date.isBefore(LocalDate.parse("9999-12-31"))) Some(date) else None
      case None       => None
    }
    Date(fromDate, toDate)
  }

  def mkSpecialties(data: ParsedFacetsProvider): List[Specialty] = {
    val specialtyOne = data.Specialty_Code1.map(code =>
      Specialty(code, Some("medicare"), Some(SpecialtyTier.One.name)))
    val specialtyTwo = data.Specialty_Code2.map(code =>
      Specialty(code, Some("medicare"), Some(SpecialtyTier.Two.name)))
    val specialtyThree = data.Specialty_Code3.map(code =>
      Specialty(code, Some("medicare"), Some(SpecialtyTier.Three.name)))
    val specialtyFour = data.Specialty_Code4.map(code =>
      Specialty(code, Some("medicare"), Some(SpecialtyTier.Four.name)))

    List(specialtyOne, specialtyTwo, specialtyThree, specialtyFour).flatten
  }

  def mkPcpFlag(data: ParsedFacetsProvider): Option[Boolean] = {
    val specialtyCodes = List(data.Specialty_Code1,
                              data.Specialty_Code2,
                              data.Specialty_Code3,
                              data.Specialty_Code4).flatten

    if (specialtyCodes.nonEmpty) {
      val pcpSpecialtyCodes: Set[String] = Set("01", "08", "11", "37", "38", "84")
      Some(specialtyCodes.exists(pcpSpecialtyCodes.contains))
    } else { None }
  }

  def mkName(data: ParsedFacetsProvider): Option[String] =
    List(data.Prov_Name_First, data.Prov_Name_Last).flatten.mkString(" ") match {
      case "" => None
      case s  => Some(s)
    }

  def mkEntityType(data: ParsedFacetsProvider): Option[String] =
    Some(
      if (data.Prov_Type.contains("INDIVIDUAL")) EntityType.individual.toString
      else EntityType.group.toString)

  def mkInNetworkFlag(data: ParsedFacetsProvider): Option[Boolean] =
    data.Network_Status.map {
      case "Y" => true
      case _   => false
    }

  def mkLocations(data: ParsedFacetsProvider): List[Location] = {
    val clinic = Address(
      address1 = data.Service_Add1,
      address2 = data.Service_Add2,
      city = data.Service_Add_City,
      state = data.Service_Add_State,
      county = None,
      zip = data.Service_Add_Zip,
      country = Some("USA"),
      phone = data.Service_Add_Phone,
      email = data.Service_Add_email
    )

    val mailing = Address(
      address1 = data.Mailing_Add1,
      address2 = data.Mailing_Add2,
      city = data.Mailing_City,
      state = data.Mailing_State,
      county = None,
      zip = data.Mailing_Zip,
      country = Some("USA"),
      phone = None,
      email = None
    )

    List(Location(Some(clinic), Some(mailing)))
  }
}
