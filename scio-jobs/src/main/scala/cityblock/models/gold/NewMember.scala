package cityblock.models.gold

import cityblock.models.Surrogate
import org.joda.time.LocalDate
import cityblock.models.gold.Claims.MemberIdentifier
import com.spotify.scio.bigquery.types.BigQueryType

object NewMember {
  @BigQueryType.toTable
  case class MemberName(
    last: Option[String],
    first: Option[String],
    middle: Option[String],
    suffix: Option[String]
  )

  @BigQueryType.toTable
  case class MemberDemographics(
    dateBirth: Option[LocalDate],
    dateDeath: Option[LocalDate],
    SSN: Option[String],
    name: MemberName,
    gender: Option[String],
    ethnicity: Option[String],
    race: Option[String],
    primaryLanguage: Option[String],
    maritalStatus: Option[String],
    subscriberRelationship: Option[String]
  )

  @BigQueryType.toTable
  case class MemberPCP(
    id: Option[String]
  )

  @BigQueryType.toTable
  case class MemberEligibility(
    lineOfBusiness: Option[String],
    subLineOfBusiness: Option[String],
    partnerBenefitPlanId: Option[String],
    partnerEmployerGroupId: Option[String]
  )

  @BigQueryType.toTable
  case class Member(
    surrogate: Surrogate,
    memberIdentifier: MemberIdentifier,
    dateEffective: Date,
    demographic: MemberDemographics,
    location: Address,
    pcp: MemberPCP,
    eligibility: MemberEligibility
  )
}
