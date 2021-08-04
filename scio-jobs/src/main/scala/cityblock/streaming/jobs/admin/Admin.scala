package cityblock.streaming.jobs.admin

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.utilities.time.DateOrInstant
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

//noinspection ScalaStyle
object Admin {
  case class VisitInsuranceInsuredAddress(
    StreetAddress: Option[String],
    City: Option[String],
    State: Option[String],
    ZIP: Option[String],
    County: Option[String],
    Country: Option[String]
  )

  object VisitInsuranceInsuredAddress {
    implicit val visitInsuranceInsuredAddressEncoder: Encoder[VisitInsuranceInsuredAddress] =
      deriveEncoder[VisitInsuranceInsuredAddress]
  }

  case class VisitInsuranceInsured(
    LastName: Option[String],
    FirstName: Option[String],
    Relationship: Option[String],
    DOB: Option[DateOrInstant],
    Sex: Option[String],
    Address: VisitInsuranceInsuredAddress
  )

  object VisitInsuranceInsured {
    implicit val visitInsuranceInsuredEncoder: Encoder[VisitInsuranceInsured] =
      deriveEncoder[VisitInsuranceInsured]
  }

  case class VisitInsuranceCompanyAddress(
    StreetAddress: Option[String],
    City: Option[String],
    State: Option[String],
    ZIP: Option[String],
    County: Option[String],
    Country: Option[String]
  )

  object VisitInsuranceCompanyAddress {
    implicit val visitInsuranceCompanyAddressEncoder: Encoder[VisitInsuranceCompanyAddress] =
      deriveEncoder[VisitInsuranceCompanyAddress]
  }

  case class VisitInsuranceCompany(
    ID: Option[String],
    IDType: Option[String],
    Name: Option[String],
    Address: VisitInsuranceCompanyAddress,
    PhoneNumber: Option[String]
  )

  object VisitInsuranceCompany {
    implicit val visitInsuranceCompanyEncoder: Encoder[VisitInsuranceCompany] =
      deriveEncoder[VisitInsuranceCompany]
  }

  case class VisitInsurancePlan(
    ID: Option[String],
    IDType: Option[String],
    Name: Option[String],
    Type: Option[String]
  )

  object VisitInsurancePlan {
    implicit val visitInsurancePlanEncoder: Encoder[VisitInsurancePlan] =
      deriveEncoder[VisitInsurancePlan]
  }

  case class VisitInsurance(
    Plan: VisitInsurancePlan,
    MemberNumber: Option[String],
    Company: VisitInsuranceCompany,
    GroupNumber: Option[String],
    GroupName: Option[String],
    EffectiveDate: Option[DateOrInstant],
    ExpirationDate: Option[DateOrInstant],
    PolicyNumber: Option[String],
    AgreementType: Option[String],
    CoverageType: Option[String],
    Insured: VisitInsuranceInsured
  )

  object VisitInsurance {
    implicit val visitInsuranceEncoder: Encoder[VisitInsurance] =
      deriveEncoder[VisitInsurance]
  }

  case class VisitGuarantorEmployerAddress(
    StreetAddress: Option[String],
    City: Option[String],
    State: Option[String],
    ZIP: Option[String],
    County: Option[String],
    Country: Option[String]
  )

  object VisitGuarantorEmployerAddress {
    implicit val visitGuarantorEmployerAddressEncoder: Encoder[VisitGuarantorEmployerAddress] =
      deriveEncoder[VisitGuarantorEmployerAddress]
  }

  case class VisitGuarantorEmployer(
    Name: Option[String],
    Address: VisitGuarantorEmployerAddress,
    PhoneNumber: Option[String]
  )

  object VisitGuarantorEmployer {
    implicit val visitGuarantorEmployerEncoder: Encoder[VisitGuarantorEmployer] =
      deriveEncoder[VisitGuarantorEmployer]
  }

  case class VisitGuarantorPhoneNumber(
    Home: Option[String],
    Business: Option[String],
    Mobile: Option[String]
  )

  object VisitGuarantorPhoneNumber {
    implicit val visitGuarantorPhoneNumberEncoder: Encoder[VisitGuarantorPhoneNumber] =
      deriveEncoder[VisitGuarantorPhoneNumber]
  }

  case class VisitGuarantorAddress(
    StreetAddress: Option[String],
    City: Option[String],
    State: Option[String],
    ZIP: Option[String],
    County: Option[String],
    Country: Option[String]
  )

  object VisitGuarantorAddress {
    implicit val visitGuarantorAddressEncoder: Encoder[VisitGuarantorAddress] =
      deriveEncoder[VisitGuarantorAddress]
  }

  case class VisitGuarantorSpouse(
    FirstName: Option[String],
    LastName: Option[String]
  )

  object VisitGuarantorSpouse {
    implicit val visitGuarantorSpouseEncoder: Encoder[VisitGuarantorSpouse] =
      deriveEncoder[VisitGuarantorSpouse]
  }

  case class VisitGuarantor(
    Number: Option[String],
    FirstName: Option[String],
    LastName: Option[String],
    DOB: Option[DateOrInstant],
    Sex: Option[String],
    Spouse: VisitGuarantorSpouse,
    Address: VisitGuarantorAddress,
    PhoneNumber: VisitGuarantorPhoneNumber,
    EmailAddresses: List[String],
    Type: Option[String],
    RelationToPatient: Option[String],
    Employer: VisitGuarantorEmployer
  )

  object VisitGuarantor {
    implicit val visitGuarantorEncoder: Encoder[VisitGuarantor] =
      deriveEncoder[VisitGuarantor]
  }

  case class VisitLocationAddress(
    StreetAddress: Option[String],
    City: Option[String],
    State: Option[String],
    ZIP: Option[String],
    County: Option[String],
    Country: Option[String]
  )

  object VisitLocationAddress {
    implicit val visitLocationAddressEncoder: Encoder[VisitLocationAddress] =
      deriveEncoder[VisitLocationAddress]
  }

  case class VisitLocation(
    Type: Option[String],
    Facility: Option[String],
    Department: Option[String],
    Room: Option[String],
    Bed: Option[String],
    Address: VisitLocationAddress
  )

  object VisitLocation {
    implicit val visitLocationEncoder: Encoder[VisitLocation] =
      deriveEncoder[VisitLocation]
  }

  case class VisitReferringProviderLocation(
    Type: Option[String],
    Facility: Option[String],
    Department: Option[String],
    Room: Option[String]
  )

  object VisitReferringProviderLocation {
    implicit val visitReferringProviderLocationEncoder: Encoder[VisitReferringProviderLocation] =
      deriveEncoder[VisitReferringProviderLocation]
  }

  case class VisitReferringProviderPhoneNumber(Office: Option[String])

  object VisitReferringProviderPhoneNumber {
    implicit val visitReferringProviderLocationEncoder: Encoder[VisitReferringProviderPhoneNumber] =
      deriveEncoder[VisitReferringProviderPhoneNumber]
  }

  case class VisitReferringProviderAddress(
    StreetAddress: Option[String],
    City: Option[String],
    State: Option[String],
    ZIP: Option[String],
    County: Option[String],
    Country: Option[String]
  )

  object VisitReferringProviderAddress {
    implicit val visitReferringProviderAddressEncoder: Encoder[VisitReferringProviderAddress] =
      deriveEncoder[VisitReferringProviderAddress]
  }

  case class VisitReferringProvider(
    ID: Option[String],
    IDType: Option[String],
    FirstName: Option[String],
    LastName: Option[String],
    Credentials: List[String],
    Address: VisitReferringProviderAddress,
    PhoneNumber: VisitReferringProviderPhoneNumber,
    Location: VisitReferringProviderLocation
  )

  object VisitReferringProvider {
    implicit val visitReferringProviderEncoder: Encoder[VisitReferringProvider] =
      deriveEncoder[VisitReferringProvider]
  }

  case class VisitConsultingProviderLocation(
    Type: Option[String],
    Facility: Option[String],
    Department: Option[String],
    Room: Option[String]
  )

  object VisitConsultingProviderLocation {
    implicit val visitConsultingProviderLocationEncoder: Encoder[VisitConsultingProviderLocation] =
      deriveEncoder[VisitConsultingProviderLocation]
  }

  case class VisitConsultingProviderPhoneNumber(Office: Option[String])

  object VisitConsultingProviderPhoneNumber {
    implicit val visitConsultingProviderPhoneNumberEncoder
      : Encoder[VisitConsultingProviderPhoneNumber] =
      deriveEncoder[VisitConsultingProviderPhoneNumber]
  }

  case class VisitConsultingProviderAddress(
    StreetAddress: Option[String],
    City: Option[String],
    State: Option[String],
    ZIP: Option[String],
    County: Option[String],
    Country: Option[String]
  )

  object VisitConsultingProviderAddress {
    implicit val visitConsultingProviderAddressEncoder: Encoder[VisitConsultingProviderAddress] =
      deriveEncoder[VisitConsultingProviderAddress]
  }

  case class VisitConsultingProvider(
    ID: Option[String],
    IDType: Option[String],
    FirstName: Option[String],
    LastName: Option[String],
    Credentials: List[String],
    Address: VisitConsultingProviderAddress,
    PhoneNumber: VisitConsultingProviderPhoneNumber,
    Location: VisitConsultingProviderLocation
  )

  object VisitConsultingProvider {
    implicit val visitConsultingProviderEncoder: Encoder[VisitConsultingProvider] =
      deriveEncoder[VisitConsultingProvider]
  }

  case class VisitAttendingProviderLocation(
    Type: Option[String],
    Facility: Option[String],
    Department: Option[String],
    Room: Option[String]
  )

  object VisitAttendingProviderLocation {
    implicit val visitAttendingProviderLocationEncoder: Encoder[VisitAttendingProviderLocation] =
      deriveEncoder[VisitAttendingProviderLocation]
  }

  case class VisitAttendingProviderPhoneNumber(Office: Option[String])

  object VisitAttendingProviderPhoneNumber {
    implicit val visitAttendingProviderPhoneNumberEncoder
      : Encoder[VisitAttendingProviderPhoneNumber] =
      deriveEncoder[VisitAttendingProviderPhoneNumber]
  }

  case class VisitAttendingProviderAddress(
    StreetAddress: Option[String],
    City: Option[String],
    State: Option[String],
    ZIP: Option[String],
    County: Option[String],
    Country: Option[String]
  )

  object VisitAttendingProviderAddress {
    implicit val visitAttendingProviderAddressEncoder: Encoder[VisitAttendingProviderAddress] =
      deriveEncoder[VisitAttendingProviderAddress]
  }

  case class VisitAttendingProvider(
    ID: Option[String],
    IDType: Option[String],
    FirstName: Option[String],
    LastName: Option[String],
    Credentials: List[String],
    Address: VisitAttendingProviderAddress,
    PhoneNumber: VisitAttendingProviderPhoneNumber,
    Location: VisitAttendingProviderLocation
  )

  object VisitAttendingProvider {
    implicit val visitAttendingProviderEncoder: Encoder[VisitAttendingProvider] =
      deriveEncoder[VisitAttendingProvider]
  }

  case class AdminVisitDischargeLocation(
    Type: Option[String],
    Facility: Option[String],
    Department: Option[String],
    Room: Option[String]
  )

  object AdminVisitDischargeLocation {
    implicit val adminVisitDischargeLocationEncoder: Encoder[AdminVisitDischargeLocation] =
      deriveEncoder[AdminVisitDischargeLocation]
  }

  case class AdminVisitDischargeStatus(
    Code: Option[String],
    Codeset: Option[String],
    Description: Option[String]
  )

  object AdminVisitDischargeStatus {
    implicit val adminVisitDischargeStatusEncoder: Encoder[AdminVisitDischargeStatus] =
      deriveEncoder[AdminVisitDischargeStatus]
  }

  case class VisitAdmissionSourceCodeableConcept(text: Option[String])

  object VisitAdmissionSourceCodeableConcept {
    implicit val visitAdmissionSourceCodeableConceptEncoder
      : Encoder[VisitAdmissionSourceCodeableConcept] =
      deriveEncoder[VisitAdmissionSourceCodeableConcept]
  }

  case class VisitAdmissionSource(
    url: Option[String],
    codeableConcept: VisitAdmissionSourceCodeableConcept
  )

  object VisitAdmissionSource {
    implicit val visitAdmissionSourceEncoder: Encoder[VisitAdmissionSource] =
      deriveEncoder[VisitAdmissionSource]
  }

  case class VisitExtensions(`admission-source`: Option[VisitAdmissionSource])

  object VisitExtensions {
    implicit val visitExtensionsEncoder: Encoder[VisitExtensions] =
      deriveEncoder[VisitExtensions]
  }

  case class AdminMessageVisit(
    VisitNumber: Option[String],
    AccountNumber: Option[String],
    PatientClass: Option[String],
    VisitDateTime: Option[DateOrInstant],
    DischargeDateTime: Option[DateOrInstant],
    DischargeStatus: Option[AdminVisitDischargeStatus],
    DischargeLocation: Option[AdminVisitDischargeLocation],
    Duration: Option[Int],
    Reason: Option[String],
    Instructions: List[Option[String]],
    Balance: Option[Long],
    DiagnosisRelatedGroup: Option[Long],
    DiagnosisRelatedGroupType: Option[Long],
    AttendingProvider: VisitAttendingProvider,
    ConsultingProvider: VisitConsultingProvider,
    ReferringProvider: VisitReferringProvider,
    Location: VisitLocation,
    Guarantor: VisitGuarantor,
    Insurances: List[VisitInsurance],
    Extensions: Option[VisitExtensions]
  )

  object AdminMessageVisit {
    implicit val adminMessageVisitEncoder: Encoder[AdminMessageVisit] =
      deriveEncoder[AdminMessageVisit]
  }

  case class PatientPCPLocation(
    Type: Option[String],
    Facility: Option[String],
    Department: Option[String],
    Room: Option[String]
  )

  object PatientPCPLocation {
    implicit val patientPCPLocationEncoder: Encoder[PatientPCPLocation] =
      deriveEncoder[PatientPCPLocation]
  }

  case class PatientPCPPhoneNumber(Office: Option[String])

  object PatientPCPPhoneNumber {
    implicit val patientPCPPhoneNumberEncoder: Encoder[PatientPCPPhoneNumber] =
      deriveEncoder[PatientPCPPhoneNumber]
  }

  case class PatientPCPAddress(
    StreetAddress: Option[String],
    City: Option[String],
    State: Option[String],
    ZIP: Option[String],
    County: Option[String],
    Country: Option[String]
  )

  object PatientPCPAddress {
    implicit val patientPCPAddressEncoder: Encoder[PatientPCPAddress] =
      deriveEncoder[PatientPCPAddress]
  }

  case class PatientPCP(
    NPI: Option[String],
    ID: Option[String],
    IDType: Option[String],
    FirstName: Option[String],
    LastName: Option[String],
    Credentials: List[String],
    Address: PatientPCPAddress,
    PhoneNumber: PatientPCPPhoneNumber,
    Location: PatientPCPLocation
  )

  object PatientPCP {
    implicit val patientPCPEncoder: Encoder[PatientPCP] =
      deriveEncoder[PatientPCP]
  }

  case class PatientAllergySeverity(
    Code: Option[String],
    Codeset: Option[String],
    Name: Option[String]
  )

  object PatientAllergySeverity {
    implicit val patientAllergySeverityEncoder: Encoder[PatientAllergySeverity] =
      deriveEncoder[PatientAllergySeverity]
  }

  case class PatientAllergyTypeReaction(
    Code: Option[String],
    Codeset: Option[String],
    Name: Option[String]
  )

  object PatientAllergyTypeReaction {
    implicit val patientAllergyTypeReactionEncoder: Encoder[PatientAllergyTypeReaction] =
      deriveEncoder[PatientAllergyTypeReaction]
  }

  case class PatientAllergyType(
    Code: Option[String],
    Codeset: Option[String],
    Name: Option[String]
  )

  object PatientAllergyType {
    implicit val patientAllergyTypeEncoder: Encoder[PatientAllergyType] =
      deriveEncoder[PatientAllergyType]
  }

  case class PatientAllergy(
    Code: Option[String],
    Codeset: Option[String],
    Name: Option[String],
    Type: PatientAllergyType,
    OnsetDateTime: Option[DateOrInstant],
    Reaction: List[PatientAllergyTypeReaction],
    Severity: PatientAllergySeverity,
    Status: Option[String]
  )

  object PatientAllergy {
    implicit val patientAllergyEncoder: Encoder[PatientAllergy] =
      deriveEncoder[PatientAllergy]
  }

  case class PatientDiagnosis(
    Code: Option[String],
    Codeset: Option[String],
    Name: Option[String],
    Type: Option[String],
    DocumentedDateTime: Option[DateOrInstant]
  )

  object PatientDiagnosis {
    implicit val patientDiagnosisEncoder: Encoder[PatientDiagnosis] =
      deriveEncoder[PatientDiagnosis]
  }

  case class PatientContactPhoneNumber(
    Home: Option[String],
    Office: Option[String],
    Mobile: Option[String]
  )

  object PatientContactPhoneNumber {
    implicit val patientContactPhoneNumberEncoder: Encoder[PatientContactPhoneNumber] =
      deriveEncoder[PatientContactPhoneNumber]
  }

  case class PatientContactAddress(
    StreetAddress: Option[String],
    City: Option[String],
    State: Option[String],
    ZIP: Option[String],
    County: Option[String],
    Country: Option[String]
  )

  object PatientContactAddress {
    implicit val patientContactPhoneNumberEncoder: Encoder[PatientContactAddress] =
      deriveEncoder[PatientContactAddress]
  }

  case class PatientContact(
    FirstName: Option[String],
    MiddleName: Option[String],
    LastName: Option[String],
    Address: PatientContactAddress,
    PhoneNumber: PatientContactPhoneNumber,
    RelationToPatient: Option[String],
    EmailAddresses: List[String],
    Roles: List[String]
  )

  object PatientContact {
    implicit val patientContactEncoder: Encoder[PatientContact] =
      deriveEncoder[PatientContact]
  }

  case class PatientDemographicsAddress(
    StreetAddress: Option[String],
    City: Option[String],
    State: Option[String],
    ZIP: Option[String],
    County: Option[String],
    Country: Option[String]
  )

  object PatientDemographicsAddress {
    implicit val patientDemographicsAddressEncoder: Encoder[PatientDemographicsAddress] =
      deriveEncoder[PatientDemographicsAddress]
  }

  case class PatientDemographicsPhoneNumber(
    Home: Option[String],
    Office: Option[String],
    Mobile: Option[String]
  )

  object PatientDemographicsPhoneNumber {
    implicit val patientDemographicsPhoneNumberEncoder: Encoder[PatientDemographicsPhoneNumber] =
      deriveEncoder[PatientDemographicsPhoneNumber]
  }

  case class PatientDemographics(
    FirstName: Option[String],
    MiddleName: Option[String],
    LastName: Option[String],
    DOB: Option[DateOrInstant],
    SSN: Option[String],
    Sex: Option[String],
    Race: Option[String],
    IsHispanic: Option[Boolean],
    MaritalStatus: Option[String],
    IsDeceased: Option[Boolean],
    DeathDateTime: Option[DateOrInstant],
    PhoneNumber: PatientDemographicsPhoneNumber,
    EmailAddresses: List[String],
    Language: Option[String],
    Citizenship: List[String],
    Address: PatientDemographicsAddress
  )

  object PatientDemographics {
    implicit val patientDemographicsEncoder: Encoder[PatientDemographics] =
      deriveEncoder[PatientDemographics]
  }

  case class PatientIdentifier(ID: String, IDType: String)

  object PatientIdentifier {
    implicit val patientIdentifierEncoder: Encoder[PatientIdentifier] =
      deriveEncoder[PatientIdentifier]
  }

  case class AdminMessagePatient(
    Identifiers: List[PatientIdentifier],
    Demographics: PatientDemographics,
    Notes: List[String],
    Contacts: List[PatientContact],
    Diagnoses: List[PatientDiagnosis],
    Allergies: List[PatientAllergy],
    PCP: PatientPCP
  )

  object AdminMessagePatient {
    implicit val adminMessagePatientEncoder: Encoder[AdminMessagePatient] =
      deriveEncoder[AdminMessagePatient]
  }

  case class MessageTransmission(ID: Option[Long])

  object MessageTransmission {
    implicit val messageTransmissionEncoder: Encoder[MessageTransmission] =
      deriveEncoder[MessageTransmission]
  }

  case class MessageMessage(ID: Option[Long])

  object MessageMessage {
    implicit val messageMessageEncoder: Encoder[MessageMessage] =
      deriveEncoder[MessageMessage]
  }

  case class MessageDestination(ID: Option[String], Name: Option[String])

  object MessageDestination {
    implicit val messageDestinationEncoder: Encoder[MessageDestination] =
      deriveEncoder[MessageDestination]
  }

  case class MessageSource(ID: Option[String], Name: Option[String])

  object MessageSource {
    implicit val messageSourceEncoder: Encoder[MessageSource] =
      deriveEncoder[MessageSource]
  }

  case class AdminMessageMeta(
    DataModel: String,
    EventType: String,
    EventDateTime: Option[DateOrInstant],
    Test: Option[Boolean],
    Source: MessageSource,
    Destinations: List[Option[MessageDestination]],
    Message: MessageMessage,
    Transmission: MessageTransmission,
    FacilityCode: Option[String]
  )

  object AdminMessageMeta {
    implicit val adminMessageMetaEncoder: Encoder[AdminMessageMeta] =
      deriveEncoder[AdminMessageMeta]
  }

  case class RawPatientAdminMessage(
    messageId: String,
    patient: Patient,
    rawAdminMessage: RawAdminMessage
  )

  object RawPatientAdminMessage {
    implicit val rawPatientAdminMessageEncoder: Encoder[RawPatientAdminMessage] =
      deriveEncoder[RawPatientAdminMessage]
  }

  case class RawAdminMessage(
    Meta: AdminMessageMeta,
    Patient: AdminMessagePatient,
    Visit: AdminMessageVisit
  )

  object RawAdminMessage {
    implicit val rawAdminMessageEncoder: Encoder[RawAdminMessage] =
      deriveEncoder[RawAdminMessage]
  }
}
