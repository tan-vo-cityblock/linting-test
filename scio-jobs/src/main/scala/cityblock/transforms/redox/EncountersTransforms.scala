package cityblock.transforms.redox

import cityblock.ehrmodels.redox.datamodel.encounter.{
  Encounter => RedoxEncounter,
  EncounterDiagnosis => RedoxEncounterDiagnosis,
  EncounterIdentifier => RedoxEncounterIdentifier,
  EncounterLocation => RedoxEncounterLocation,
  EncounterLocationAddress => RedoxEncounterLocationAddress,
  EncounterLocationAddressType => RedoxEncounterLocationAddressType,
  EncounterProvider => RedoxEncounterProvider,
  EncounterProviderRole => RedoxEncounterProviderRole,
  EncounterReason => RedoxEncounterReason,
  EncounterType => RedoxEncounterType
}
import cityblock.member.service.models.PatientInfo.Patient
import cityblock.streaming.jobs.aggregations.Encounters._
import cityblock.utilities.Environment
import org.joda.time.Instant

trait TransformEncounters {
  def makeEncounterIdentifier(redoxIdentifier: RedoxEncounterIdentifier): List[Identifier] =
    redoxIdentifier match {
      case RedoxEncounterIdentifier(None, None) => List()
      case identifier =>
        List(
          Identifier(identifier.ID, identifier.IDType)
        )
    }

  def makeEncounterReason(redoxReason: RedoxEncounterReason): List[EncounterReason] =
    redoxReason match {
      case RedoxEncounterReason(None, None, None, None) => List()
      case reason =>
        List(
          EncounterReason(
            code = reason.Code,
            codeSystem = reason.CodeSystem,
            codeSystemName = reason.CodeSystemName,
            name = reason.Name,
            notes = None
          )
        )
    }

  def makeEncounterDiagnosis(redoxDiagnosis: RedoxEncounterDiagnosis): List[EncounterDiagnosis] =
    redoxDiagnosis match {
      case RedoxEncounterDiagnosis(None, None, None, None) => List()
      case diagnosis =>
        List(
          EncounterDiagnosis(
            code = diagnosis.Code,
            codeSystem = diagnosis.CodeSystem,
            codeSystemName = diagnosis.CodeSystemName,
            name = diagnosis.Name
          )
        )
    }

  def makeEncounterLocation(location: RedoxEncounterLocation): List[EncounterLocation] = {
    val address = location.Address match {
      case None | Some(RedoxEncounterLocationAddress(None, None, None, None, None)) => None
      case Some(redoxAddress) =>
        Some(
          Address(
            street1 = redoxAddress.StreetAddress,
            street2 = None,
            city = redoxAddress.City,
            state = redoxAddress.State,
            zip = redoxAddress.ZIP
          )
        )
    }

    val locationType = location.Type match {
      case None | Some(RedoxEncounterLocationAddressType(None, None, None, None)) =>
        None
      case Some(locType) =>
        Some(
          EncounterLocationAddressType(
            code = locType.Code,
            codeSystem = locType.CodeSystem,
            codeSystemName = locType.CodeSystemName,
            name = locType.Name
          )
        )
    }

    (address, locationType, location.Name) match {
      case (None, None, None) => List()
      case (locAddress, locType, locName) =>
        List(
          EncounterLocation(address = locAddress, `type` = locType, name = locName)
        )
    }
  }

  def makeEncounterProvider(provider: RedoxEncounterProvider): EncounterProvider = {
    val providerIds = (provider.ID, provider.IDType) match {
      case (None, None) => List()
      case (id, idType) => List(Identifier(id, idType))
    }
    // TODO: Append CBH UserID to provierIds.

    val credentials = provider.Credentials.fold(List[String]())(identity)

    val providerAddress = provider.Address match {
      case None => None
      case Some(address) =>
        Some(
          Address(
            street1 = address.StreetAddress,
            street2 = None,
            city = address.City,
            state = address.State,
            zip = address.ZIP
          ))
    }

    val providerPhone = Some(provider.PhoneNumber.flatMap(_.Office).getOrElse(""));

    val providerRole = provider.Role match {
      case None | Some(RedoxEncounterProviderRole(None, None, None, None)) => None
      case Some(role) =>
        Some(
          EncounterProviderRole(
            code = role.Code,
            codeSystem = role.CodeSystem,
            codeSystemName = role.CodeSystemName,
            name = role.Name
          )
        )
    }

    EncounterProvider(
      identifiers = providerIds,
      firstName = provider.FirstName,
      lastName = provider.LastName,
      credentials = credentials,
      address = providerAddress,
      phone = providerPhone,
      role = providerRole
    )
  }

  def makeEncounterType(encounterType: RedoxEncounterType): EncounterType =
    EncounterType(
      code = encounterType.Code,
      codeSystem = encounterType.CodeSystem,
      codeSystemName = encounterType.CodeSystemName,
      name = encounterType.Name
    )
}

trait EnrichEncounters {
  def makeEnrichedEncounter(externalId: String, encounter: Encounter)(
    implicit environment: Environment
  ): Encounter =
    if (encounter.endDateTime.isEmpty && encounter.dateTime.isEmpty) {
      encounter
    } else {
      EnrichEncounter.addEncounterDetails(externalId, encounter)
    }
}

object EncountersTransforms extends TransformEncounters with EnrichEncounters {
  def transformRedoxEncounterToPatientEncounter(
    messageId: String,
    insertedAt: Instant,
    isStreaming: Boolean,
    patient: Patient,
    redoxEncounter: RedoxEncounter
  )(implicit environment: Environment): PatientEncounter = {
    val encounterIds: List[Identifier] =
      redoxEncounter.Identifiers.flatMap(makeEncounterIdentifier)

    val encounterReason: List[EncounterReason] =
      redoxEncounter.ReasonForVisit.flatMap(makeEncounterReason)

    val encounterDiagnosis: List[EncounterDiagnosis] =
      redoxEncounter.Diagnosis.flatMap(makeEncounterDiagnosis)

    val locations: List[EncounterLocation] =
      redoxEncounter.Locations.flatMap(makeEncounterLocation)

    val encounterProviders: List[EncounterProvider] =
      redoxEncounter.Providers.map(makeEncounterProvider)

    val encounterType: EncounterType = makeEncounterType(redoxEncounter.Type)

    val encounter = Encounter(
      identifiers = encounterIds,
      `type` = encounterType,
      dateTime = redoxEncounter.DateTime,
      endDateTime = redoxEncounter.EndDateTime,
      providers = encounterProviders,
      locations = locations,
      diagnoses = encounterDiagnosis,
      reasons = encounterReason,
      draft = false
    )

    val encounterWithEpicVisitNote = makeEnrichedEncounter(patient.externalId, encounter)

    PatientEncounter(
      messageId = messageId,
      insertedAt = Some(insertedAt),
      isStreaming = isStreaming,
      patient = patient,
      encounter = encounterWithEpicVisitNote
    )
  }
}
