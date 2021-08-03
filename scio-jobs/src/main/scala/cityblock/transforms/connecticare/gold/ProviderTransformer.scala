package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.{ProviderAddress, ProviderDetail}
import cityblock.models.Surrogate
import cityblock.models.connecticare.silver.ProviderDetail.ParsedProviderDetail
import cityblock.models.gold.Claims.EntityType
import cityblock.models.gold.NewProvider._
import cityblock.models.gold.{Address, Date}
import cityblock.transforms.Transform
import cityblock.transforms.Transform.Transformer
import com.spotify.scio.values.SCollection
import org.joda.time.LocalDate

case class ProviderTransformer(
  detailsCom: SCollection[ProviderDetail],
  addressesCom: SCollection[ProviderAddress],
  deliveryDate: LocalDate
) extends Transformer[Provider] {
  override def transform()(implicit pr: String): SCollection[Provider] =
    ProviderTransformer.pipeline(pr, detailsCom, addressesCom, deliveryDate)
}

object ProviderTransformer {
  import scala.language.implicitConversions

  implicit private def detailGetSurrogateId(detail: ProviderDetail): String =
    detail.identifier.surrogateId

  implicit private def addressGetSurrogateId(address: ProviderAddress): String =
    address.identifier.surrogateId

  /**
   * Logic: if the dateEffective.to date is empty, assume it is in the future (meaning this row is the last)
   */
  val ProviderToDateOrdering: Ordering[Option[LocalDate]] =
    Ordering.fromLessThan[Option[LocalDate]] {
      case (Some(l), Some(r)) => l.compareTo(r) == -1
      case (Some(_), None)    => true
      case (None, Some(_))    => false
      case _                  => true
    }

  def pipeline(project: String,
               detailsCom: SCollection[ProviderDetail],
               addressesCom: SCollection[ProviderAddress],
               deliveryDate: LocalDate): SCollection[Provider] = {
    val indexedProviderLocations =
      addressesCom
        .withName("Add surrogates to ProviderAddress")
        .map {
          Transform.addSurrogate(project, "silver_claims", "Provider_Address", _)
        }
        .withName("Create ProviderLocations and index by ProvNum")
        .map {
          case (surrogate, row) =>
            (ProviderKey(row), mkProviderLocation(row, surrogate))
        }
        .withName("Filter down duplicate addresses")
        .distinct
        .withName("Group ProviderLocations by ProvNum")
        .groupByKey

    val providersPartlyGold = detailsCom
      .withName("Add surrogates to ProviderDetail")
      .map {
        Transform.addSurrogate(project, "silver_claims", "ProviderDetail", _)
      }
      .withName("Remove duplicate rows")
      .distinctBy {
        case (_, detail) => detail.detail
      }
      .withName("Index providers by ProvNum")
      .map {
        case (surrogate, detail) =>
          (ProviderKey(detail), (surrogate, detail))
      }
      .withName("Join with indexed addresses")
      .leftOuterJoin(indexedProviderLocations)
      .withName("Create provider index")
      .map {
        case (_, ((surrogate, detail), maybeLocations)) =>
          val locations = maybeLocations match {
            case Some(xs) => xs
            case _        => List()
          }
          mkProvider(detail, surrogate, deliveryDate, locations)
      }
      .withName("Remove duplicate rows")
      .distinctBy {
        _.copy(providerIdentifier = ProviderIdentifier(Surrogate("", "", "", ""), "", "", ""))
      }

    /**
     * Adjust dateEffective.from for providers with multiple rows.
     * Business logic: set dateEffective.to as the same provider's previous dateEffective.to + 1 day
     */
    val providersGold = providersPartlyGold
      .groupBy(_.providerIdentifier.id)
      .values
      .map { providerRows =>
        providerRows.toList
          .sortBy(_.dateEffective.to)(ProviderToDateOrdering)
          .sliding(2)
      }
      .flatMap { windows =>
        val windowsList = windows.toList
        val firstProvRow = windowsList.head.head

        /**
         * Each window has two window panes: the row with the previous dateEffective.to, and the row we are changing
         * Add one day to the first pane's dateEffective.to
         */
        val nextProvRows = windowsList
          .filter(_.tail.nonEmpty)
          .map { window =>
            val nextStartDate = window.head.dateEffective.to match {
              case None       => None
              case Some(date) => Some(date.plusDays(1))
            }

            val windowPane = window.tail.head

            windowPane.copy(dateEffective = windowPane.dateEffective.copy(from = nextStartDate))
          }

        Seq(firstProvRow) ++ nextProvRows
      }

    providersGold

  }

  def mkProvider(detail: ProviderDetail,
                 surrogate: Surrogate,
                 deliveryDate: LocalDate,
                 locations: Iterable[List[Location]]): Provider =
    Provider(
      providerIdentifier = mkIdentifier(surrogate, detail),
      dateEffective = mkEndDate(detail.detail, deliveryDate),
      specialties = mkSpecialties(detail.detail),
      taxonomies = List(),
      pcpFlag = mkPcpFlag(detail.detail),
      npi = detail.detail.ProvNPI,
      name = mkName(detail.detail),
      affiliatedGroupName = detail.detail.ProvTINName,
      entityType = mkEntityType(detail.detail),
      inNetworkFlag = mkInNetworkFlag(detail.detail),
      locations = locations.flatten.toList
    )

  def mkIdentifier(surrogate: Surrogate, silver: ProviderDetail): ProviderIdentifier =
    ProviderIdentifier(
      surrogate = surrogate,
      partnerProviderId = silver.detail.ProvNum,
      partner = partner,
      id = ProviderKey(silver.detail.ProvNum).uuid.toString
    )

  /**
   * Set fromDate to None. This will be overwritten.
   */
  def mkEndDate(detail: ParsedProviderDetail, deliveryDate: LocalDate): Date = {
    val fromDate: Option[LocalDate] = Some(new LocalDate(2000, 1, 1))
    val toDate: Option[LocalDate] =
      detail.DateEnd.flatMap(end => if (end.isBefore(deliveryDate)) Some(end) else None)
    Date(fromDate, toDate)
  }

  def mkSpecialties(detail: ParsedProviderDetail): List[Specialty] =
    detail.ProvSpec1.map(code => Specialty(code, Some("medicare"), None)).toList

  def mkPcpFlag(detail: ParsedProviderDetail): Option[Boolean] = {
    val providerSpecialty: Option[String] = detail.ProvSpec1
    val pcpSpecialtyCodes: Set[String] = Set("01", "08", "11", "37", "38", "84")

    if (providerSpecialty.isDefined) {
      val isPcp: Boolean = providerSpecialty.exists(pcpSpecialtyCodes.contains)
      Some(isPcp)
    } else { None }
  }

  def mkName(detail: ParsedProviderDetail): Option[String] = {
    val providerName =
      List(detail.ProvFirstName, detail.ProvLastName).flatten.mkString(" ")

    Some(providerName)
  }

  def mkEntityType(detail: ParsedProviderDetail): Option[String] =
    if (detail.HATCd.contains("OT")) {
      Some(EntityType.group.toString)
    } else {
      Some(EntityType.individual.toString)
    }

  def mkInNetworkFlag(detail: ParsedProviderDetail): Option[Boolean] = {
    val codesINN: Set[String] = Set("IN", "IL", "IS", "IT", "ID")

    if (detail.ProvStatus.isDefined) {
      val isINN: Boolean = detail.ProvStatus.exists(codesINN.contains)
      Some(isINN)
    } else { None }
  }

  def mkProviderLocation(address: ProviderAddress, surrogate: Surrogate): List[Location] = {
    val clinicLocation = Address(
      address1 = address.address.Address1,
      address2 = address.address.Address2,
      city = address.address.City,
      state = address.address.State,
      county = address.address.County,
      zip = address.address.ZipCd,
      country = None,
      email = None,
      phone = None
    )

    val mailingLocation = None

    List(Location(Some(clinicLocation), mailingLocation))
  }
}
