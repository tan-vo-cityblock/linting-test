package cityblock.models.gold

import cityblock.models.Surrogate
import cityblock.models.gold.Claims.{Diagnosis, MemberIdentifier, Procedure}
import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

/**
 * Gold model for facility claims. The case classes are wrapped by an object because the
 * [[BigQueryType.toTable]] macro requires annotated classes to have a parent object.
 */
object FacilityClaim {

  @BigQueryType.toTable
  case class Facility(
    claimId: String,
    memberIdentifier: MemberIdentifier,
    header: Header,
    lines: List[Line]
  )

  @BigQueryType.toTable
  case class PreCombinedFacility(
    claimId: String,
    memberIdentifier: MemberIdentifier,
    header: Header,
    lines: List[PreCombinedLine]
  )

  @BigQueryType.toTable
  case class Header(
    partnerClaimId: String,
    typeOfBill: Option[String],
    admissionType: Option[String],
    admissionSource: Option[String],
    dischargeStatus: Option[String],
    lineOfBusiness: Option[String],
    subLineOfBusiness: Option[String],
    drg: DRG,
    provider: HeaderProvider,
    diagnoses: List[Diagnosis],
    procedures: List[HeaderProcedure],
    date: Date
  )

  @BigQueryType.toTable
  case class HeaderProcedure(
    surrogate: Surrogate,
    tier: String,
    codeset: String,
    code: String
  )

  @BigQueryType.toTable
  case class DRG(
    version: Option[String],
    codeset: Option[String],
    code: Option[String]
  )

  @BigQueryType.toTable
  case class HeaderProvider(
    billing: Option[ProviderIdentifier],
    referring: Option[ProviderIdentifier],
    servicing: Option[ProviderIdentifier],
    operating: Option[ProviderIdentifier]
  )

  @BigQueryType.toTable
  case class Date(
    from: Option[LocalDate],
    to: Option[LocalDate],
    admit: Option[LocalDate],
    discharge: Option[LocalDate],
    paid: Option[LocalDate]
  )

  @BigQueryType.toTable
  case class Line(
    surrogate: Surrogate,
    lineNumber: Int,
    revenueCode: Option[String],
    cobFlag: Option[Boolean],
    capitatedFlag: Option[Boolean],
    claimLineStatus: Option[String],
    inNetworkFlag: Option[Boolean],
    serviceQuantity: Option[Int],
    typesOfService: List[TypeOfService],
    procedure: Option[Procedure],
    amount: Amount,
  )

  @BigQueryType.toTable
  case class PreCombinedLine(
    surrogate: Surrogate,
    lineNumber: Int,
    revenueCode: Option[String],
    cobFlag: Option[Boolean],
    capitatedFlag: Option[Boolean],
    claimLineStatus: Option[String],
    inNetworkFlag: Option[Boolean],
    serviceQuantity: Option[Int],
    typesOfService: List[TypeOfService],
    procedure: Option[Procedure],
    amount: Amount,
    Version_Number: Option[String]
  )
}
