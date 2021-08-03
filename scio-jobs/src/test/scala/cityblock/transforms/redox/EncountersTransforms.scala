package cityblock.transforms.redox

import cityblock.ehrmodels.redox.datamodel.encounter.{Encounter => RedoxEncounter}
import cityblock.member.service.models.PatientInfo.Patient
import cityblock.streaming.jobs.aggregations.Encounters._
import cityblock.utilities.Environment
import org.joda.time.Instant
import org.scalamock.scalatest.MockFactory

object EncountersTransforms extends TransformEncounters with MockFactory {
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

    val mockEnrichEncounters: EnrichEncounters = stub[EnrichEncounters]

    (mockEnrichEncounters
      .makeEnrichedEncounter(_: String, _: Encounter)(_: Environment))
      .when(patient.externalId, encounter, *)
      .returns(encounter)

    val encounterWithEpicVisitNote =
      mockEnrichEncounters.makeEnrichedEncounter(patient.externalId, encounter)

    PatientEncounter(
      messageId = messageId,
      insertedAt = Some(insertedAt),
      isStreaming = isStreaming,
      patient = patient,
      encounter = encounterWithEpicVisitNote
    )
  }
}
