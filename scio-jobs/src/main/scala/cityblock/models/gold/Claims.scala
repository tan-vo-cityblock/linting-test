package cityblock.models.gold

import cityblock.models.{Identifier, Surrogate}
import com.spotify.scio.bigquery.description
import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object Claims {
  object ProviderLocationType extends Enumeration {
    type LocationType = Value
    val Servicing: LocationType = Value("servicing")
    val Mailing: LocationType = Value("billing")
    val Unknown: LocationType = Value("unknown")
  }

  object ClaimLineStatus extends Enumeration {
    type ClaimLineStatus = Value
    val Paid, Reversed, Denied, Open, Voided, Encounter, Unknown, Adjusted,
    Reissue: ClaimLineStatus = Value
  }

  object Gender extends Enumeration {
    type Gender = Value
    val M, F, U = Value
  }

  object EntityType extends Enumeration {
    type EntityType = Value
    val individual, group = Value
  }

  @BigQueryType.toTable
  case class ProviderLocation(
    surrogate: Surrogate,
    locationType: String,
    address1: Option[String],
    address2: Option[String],
    city: Option[String],
    state: Option[String],
    zip: Option[String],
    email: Option[String],
    phone: Option[String]
  )

  @BigQueryType.toTable
  case class ProviderProfessional(
    lastName: Option[String],
    firstName: Option[String],
    middleName: Option[String],
    gender: Option[String],
    race: Option[String],
    ethnicity: Option[String],
    language: Option[String],
    licenseNumber: Option[String],
    taxonomy: Option[String]
  )

  @BigQueryType.toTable
  case class ProviderOrganization(
    name: Option[String]
  )

  @BigQueryType.toTable
  case class Provider(
    identifier: Identifier,
    providerType: Option[String],
    NPI: Option[String],
    TIN: Option[String],
    inNetwork: Option[Boolean],
    NCPDPId: Option[String],
    professional: Option[ProviderProfessional],
    organization: Option[ProviderOrganization],
    locations: List[ProviderLocation]
  )

  @BigQueryType.toTable
  case class MemberAttributionDates(
    from: Option[LocalDate],
    to: Option[LocalDate]
  )

  @BigQueryType.toTable
  case class MemberAttribution(
    identifier: Identifier,
    date: MemberAttributionDates,
    PCPId: Option[String]
  )

  @BigQueryType.toTable
  case class MemberDemographicsDates(
    from: Option[LocalDate],
    to: Option[LocalDate],
    birth: Option[LocalDate],
    death: Option[LocalDate]
  )

  @BigQueryType.toTable
  case class MemberDemographicsIdentity(
    lastName: Option[String],
    firstName: Option[String],
    middleName: Option[String],
    nameSuffix: Option[String],
    gender: Option[String],
    ethnicity: Option[String],
    race: Option[String],
    primaryLanguage: Option[String],
    maritalStatus: Option[String],
    relation: Option[String],
    SSN: Option[String],
    NMI: String
  )

  @BigQueryType.toTable
  case class MemberDemographicsLocation(
    address1: Option[String],
    address2: Option[String],
    city: Option[String],
    state: Option[String],
    county: Option[String],
    zip: Option[String],
    email: Option[String],
    phone: Option[String]
  )

  @BigQueryType.toTable
  case class MemberDemographics(
    identifier: Identifier,
    date: MemberDemographicsDates,
    identity: MemberDemographicsIdentity,
    location: MemberDemographicsLocation
  )

  @BigQueryType.toTable
  case class MemberEligibilityDates(
    from: Option[LocalDate],
    to: Option[LocalDate]
  )

  @BigQueryType.toTable
  case class MemberEligibilityDetail(
    lineOfBusiness: Option[String],
    subLineOfBusiness: Option[String],
    partnerPlanDescription: Option[String],
    partnerBenefitPlanId: Option[String],
    partnerEmployerGroupId: Option[String]
  )

  @BigQueryType.toTable
  case class MemberEligibility(
    identifier: Identifier,
    date: MemberEligibilityDates,
    detail: MemberEligibilityDetail
  )

  @BigQueryType.toTable
  case class MemberIdentifier(
    @description(
      "Global identifier for all members ingested by Cityblock. Unique across silver and gold.")
    commonId: Option[String],
    @description("Partner-specific member id.")
    partnerMemberId: String,
    @description("Global identifier for patients in a Cityblock cohort (shared with Commons).")
    patientId: Option[String],
    @description("Name of partner as it appears in the member index.")
    partner: String
  )

  @BigQueryType.toTable
  case class Member(
    identifier: MemberIdentifier,
    demographics: MemberDemographics,
    attributions: List[MemberAttribution],
    eligibilities: List[MemberEligibility],
  )

  @BigQueryType.toTable
  case class Diagnosis(
    surrogate: Surrogate,
    tier: String,
    codeset: String,
    code: String
  )

  @BigQueryType.toTable
  case class Procedure(
    surrogate: Surrogate,
    tier: String,
    codeset: String,
    code: String,
    modifiers: List[String]
  )

  @BigQueryType.toTable
  case class LabResult(
    identifier: Identifier,
    memberIdentifier: MemberIdentifier,
    name: Option[String],
    result: Option[String],
    resultNumeric: Option[BigDecimal],
    units: Option[String],
    loinc: Option[String],
    date: Option[LocalDate],
    procedure: Option[Procedure]
  )
}
