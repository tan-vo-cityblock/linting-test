package cityblock.models.gold

import cityblock.models.Identifier
import cityblock.models.gold.Claims.MemberIdentifier
import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object PriorAuthorization {
  object AuthorizationServiceStatus extends Enumeration {
    type AuthorizationServiceStatus = Value
    val Approved = Value
  }

  @BigQueryType.toTable
  case class PriorAuthorization(
    identifier: Identifier,
    memberIdentifier: MemberIdentifier,
    authorizationId: String,
    authorizationStatus: Option[String],
    placeOfService: Option[String],
    careType: Option[String],
    caseType: Option[String],
    admitDate: Option[LocalDate],
    dischargeDate: Option[LocalDate],
    serviceStartDate: Option[LocalDate],
    serviceEndDate: Option[LocalDate],
    facilityIdentifier: Facility,
    diagnosis: List[Diagnosis],
    procedures: List[Procedure],
    servicingProvider: ProviderIdentifier,
    requestingProvider: ProviderIdentifier,
    serviceStatus: Option[String],
    serviceType: Option[String],
    requestDate: Option[LocalDate],
    statusReason: Option[String],
    recordCreatedDate: Option[LocalDate],
    recordUpdatedDate: Option[LocalDate]
  )

  @BigQueryType.toTable
  case class Procedure(
    code: Option[String],
    modifiers: List[String],
    serviceUnits: Option[String],
    unitType: Option[String]
  )

  case class Diagnosis(
    diagnosisCode: Option[String],
    diagnosisDescription: Option[String]
  )

  @BigQueryType.toTable
  case class ProviderIdentifier(
    providerId: Option[String],
    partnerProviderId: Option[String],
    providerNPI: Option[String],
    specialty: Option[String],
    providerName: Option[String]
  )

  case class Facility(
    facilityNPI: Option[String],
    facilityName: Option[String],
    facilityAddress: Address
  )
}
