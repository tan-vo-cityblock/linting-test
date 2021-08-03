package cityblock.ehrmodels.redox.datamodel.encounter

import io.circe.generic.JsonCodec

/**
 * params - See Encounters section in Redox docs for up-to-date description of Encounter model fields.
 * https://developer.redoxengine.com/data-models/ClinicalSummary.html#PatientQueryResponse
 */
@JsonCodec
case class EncounterIdentifier(
  ID: Option[String],
  IDType: Option[String],
)

@JsonCodec
case class EncounterType(
  Code: String,
  CodeSystem: Option[String],
  CodeSystemName: Option[String],
  Name: Option[String],
)

@JsonCodec
case class EncounterProviderPhoneNumber(Office: Option[String])

@JsonCodec
case class EncounterProviderAddress(
  StreetAddress: Option[String],
  City: Option[String],
  State: Option[String],
  ZIP: Option[String],
  County: Option[String],
  Country: Option[String]
)

@JsonCodec
case class EncounterProviderLocation(Type: Option[String],
                                     Facility: Option[String],
                                     Department: Option[String],
                                     Room: Option[String])

@JsonCodec
case class EncounterProviderRole(Code: Option[String],
                                 CodeSystem: Option[String],
                                 CodeSystemName: Option[String],
                                 Name: Option[String])
@JsonCodec
case class EncounterProvider(
  ID: Option[String],
  IDType: Option[String],
  FirstName: Option[String],
  LastName: Option[String],
  Credentials: Option[List[String]],
  Address: Option[EncounterProviderAddress],
  Location: Option[EncounterProviderLocation],
  PhoneNumber: Option[EncounterProviderPhoneNumber],
  Role: Option[EncounterProviderRole]
)

@JsonCodec
case class EncounterLocationAddress(StreetAddress: Option[String],
                                    City: Option[String],
                                    State: Option[String],
                                    Country: Option[String],
                                    ZIP: Option[String])

@JsonCodec
case class EncounterLocationAddressType(
  Code: Option[String],
  CodeSystem: Option[String],
  CodeSystemName: Option[String],
  Name: Option[String]
)

@JsonCodec
case class EncounterLocation(
  Address: Option[EncounterLocationAddress],
  Type: Option[EncounterLocationAddressType],
  Name: Option[String]
)

@JsonCodec
case class EncounterDiagnosis(
  Code: Option[String],
  CodeSystem: Option[String],
  CodeSystemName: Option[String],
  Name: Option[String]
)

@JsonCodec
case class EncounterReason(
  Code: Option[String],
  CodeSystem: Option[String],
  CodeSystemName: Option[String],
  Name: Option[String]
)
