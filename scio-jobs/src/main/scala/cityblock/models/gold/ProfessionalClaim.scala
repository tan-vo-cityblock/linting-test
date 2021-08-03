package cityblock.models.gold

import cityblock.models.Surrogate
import cityblock.models.gold.Claims.{Diagnosis, MemberIdentifier, Procedure}
import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

/**
 * Gold model for professional claims. The case classes are wrapped by an object because the
 * [[BigQueryType.toTable]] macro requires annotated classes to have a parent object.
 */
object ProfessionalClaim {

  @BigQueryType.toTable
  case class Professional(
    claimId: String,
    memberIdentifier: MemberIdentifier,
    header: Header,
    lines: List[Line]
  )

  @BigQueryType.toTable
  case class PreCombinedProfessional(
    claimId: String,
    memberIdentifier: MemberIdentifier,
    header: Header,
    lines: List[PreCombinedLine]
  )
  @BigQueryType.toTable
  case class Header(
    partnerClaimId: String,
    lineOfBusiness: Option[String],
    subLineOfBusiness: Option[String],
    provider: HeaderProvider,
    diagnoses: List[Diagnosis]
  )

  @BigQueryType.toTable
  case class HeaderProvider(
    billing: Option[ProviderIdentifier],
    referring: Option[ProviderIdentifier]
  )

  @BigQueryType.toTable
  case class Line(
    surrogate: Surrogate,
    lineNumber: Int,
    cobFlag: Option[Boolean],
    capitatedFlag: Option[Boolean],
    claimLineStatus: Option[String],
    inNetworkFlag: Option[Boolean],
    serviceQuantity: Option[Int],
    placeOfService: Option[String],
    date: ProfessionalDate,
    provider: LineProvider,
    procedure: Option[Procedure],
    amount: Amount,
    diagnoses: List[Diagnosis],
    typesOfService: List[TypeOfService],
  )

  @BigQueryType.toTable
  case class PreCombinedLine(
    surrogate: Surrogate,
    lineNumber: Int,
    cobFlag: Option[Boolean],
    capitatedFlag: Option[Boolean],
    claimLineStatus: Option[String],
    inNetworkFlag: Option[Boolean],
    serviceQuantity: Option[Int],
    placeOfService: Option[String],
    date: ProfessionalDate,
    provider: LineProvider,
    procedure: Option[Procedure],
    amount: Amount,
    diagnoses: List[Diagnosis],
    typesOfService: List[TypeOfService],
    Version_Number: Option[String]
  )

  @BigQueryType.toTable
  case class ProfessionalDate(
    from: Option[LocalDate],
    to: Option[LocalDate],
    paid: Option[LocalDate]
  )

  @BigQueryType.toTable
  case class LineProvider(
    servicing: Option[ProviderIdentifier]
  )
}
