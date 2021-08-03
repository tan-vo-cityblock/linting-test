package cityblock.streaming.jobs.aggregations

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.utilities.time.DateOrInstant
import com.spotify.scio.bigquery.types.BigQueryType
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import org.joda.time.Instant

object Encounters {
  @BigQueryType.toTable
  case class Identifier(
    id: Option[String],
    idType: Option[String]
  )

  object Identifier {
    implicit val identifierEncoder: Encoder[Identifier] =
      deriveEncoder[Identifier]
  }

  @BigQueryType.toTable
  case class Address(
    street1: Option[String],
    street2: Option[String],
    city: Option[String],
    state: Option[String],
    zip: Option[String]
  )

  object Address {
    implicit val addressEncoder: Encoder[Address] =
      deriveEncoder[Address]
  }

  @BigQueryType.toTable
  case class EncounterReason(
    code: Option[String],
    codeSystem: Option[String],
    codeSystemName: Option[String],
    name: Option[String],
    notes: Option[String]
  )

  object EncounterReason {
    implicit val encounterEncoder: Encoder[EncounterReason] =
      deriveEncoder[EncounterReason]
  }

  @BigQueryType.toTable
  case class EncounterDiagnosis(
    code: Option[String],
    codeSystem: Option[String],
    codeSystemName: Option[String],
    name: Option[String]
  )

  object EncounterDiagnosis {
    implicit val encounterDiagnosisEncoder: Encoder[EncounterDiagnosis] =
      deriveEncoder[EncounterDiagnosis]
  }

  @BigQueryType.toTable
  case class EncounterLocationAddressType(
    code: Option[String],
    codeSystem: Option[String],
    codeSystemName: Option[String],
    name: Option[String]
  )

  object EncounterLocationAddressType {
    implicit val encounterLocationAddressEncoder: Encoder[EncounterLocationAddressType] =
      deriveEncoder[EncounterLocationAddressType]
  }

  @BigQueryType.toTable
  case class EncounterLocation(
    address: Option[Address],
    `type`: Option[EncounterLocationAddressType],
    name: Option[String]
  )

  object EncounterLocation {
    implicit val encounterLocationEncoder: Encoder[EncounterLocation] =
      deriveEncoder[EncounterLocation]
  }

  @BigQueryType.toTable
  case class EncounterProviderRole(
    code: Option[String],
    codeSystem: Option[String],
    codeSystemName: Option[String],
    name: Option[String]
  )

  object EncounterProviderRole {
    implicit val encounterProviderRoleEncoder: Encoder[EncounterProviderRole] =
      deriveEncoder[EncounterProviderRole]
  }

  @BigQueryType.toTable
  case class EncounterProvider(
    identifiers: List[Identifier],
    firstName: Option[String],
    lastName: Option[String],
    credentials: List[String],
    address: Option[Address],
    phone: Option[String],
    role: Option[EncounterProviderRole]
  )

  object EncounterProvider {
    implicit val encounterProviderEncoder: Encoder[EncounterProvider] =
      deriveEncoder[EncounterProvider]
  }

  @BigQueryType.toTable
  case class EncounterType(
    code: String,
    codeSystem: Option[String],
    codeSystemName: Option[String],
    name: Option[String]
  )

  object EncounterType {
    implicit val encounterTypeEncoder: Encoder[EncounterType] =
      deriveEncoder[EncounterType]
  }

  @BigQueryType.toTable
  case class Encounter(
    identifiers: List[Identifier],
    `type`: EncounterType,
    dateTime: Option[DateOrInstant],
    endDateTime: Option[DateOrInstant],
    providers: List[EncounterProvider],
    locations: List[EncounterLocation],
    diagnoses: List[EncounterDiagnosis],
    reasons: List[EncounterReason],
    draft: Boolean
  )

  object Encounter {
    implicit val encounterEncoder: Encoder[Encounter] =
      deriveEncoder[Encounter]
  }

  @BigQueryType.toTable
  case class PatientEncounter(
    messageId: String,
    insertedAt: Option[Instant],
    isStreaming: Boolean,
    patient: Patient,
    encounter: Encounter
  )

  object PatientEncounter {
    implicit val instantEncoder: Encoder[Instant] =
      Encoder.encodeString.contramap[Instant](_.toString)
    implicit val patientEncounterEncoder: Encoder[PatientEncounter] =
      deriveEncoder[PatientEncounter]
  }
}
