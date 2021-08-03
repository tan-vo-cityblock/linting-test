package cityblock.aggregators.jobs

import PushElationPatientEncountersTest._
import cityblock.ehrmodels.elation.datamodeldb.practice.ServiceLocationTable._
import cityblock.ehrmodels.elation.datamodeldb.bill.BillTable._
import cityblock.ehrmodels.elation.datamodeldb.bill.BillItemTable._
import cityblock.ehrmodels.elation.datamodeldb.bill.BillItemDxTable._
import cityblock.ehrmodels.elation.datamodeldb.documenttag.DocumentTagTable._
import cityblock.ehrmodels.elation.datamodeldb.icd.Icd10Table._
import cityblock.ehrmodels.elation.datamodeldb.icd.Icd9Table._
import cityblock.ehrmodels.elation.datamodeldb.nonvisitnote.NonVisitNoteTable._
import cityblock.ehrmodels.elation.datamodeldb.nonvisitnote.NonVisitNoteBulletTable._
import cityblock.ehrmodels.elation.datamodeldb.nonvisitnote.NonVisitNoteDocumentTagTable._
import cityblock.ehrmodels.elation.datamodeldb.practice.PracticeTable._
import cityblock.ehrmodels.elation.datamodeldb.user.UserTable._
import cityblock.ehrmodels.elation.datamodeldb.visitnote.VisitNoteTable._
import cityblock.ehrmodels.elation.datamodeldb.visitnote.VisitNoteBulletTable._
import cityblock.member.service.io.ReadMrnPatients
import cityblock.member.service.models.PatientInfo.{Datasource, Patient}
import cityblock.streaming.jobs.aggregations.Encounters._
import cityblock.streaming.jobs.aggregations.Encounters.{Address => EncounterAdress}
import cityblock.utilities.Environment
import cityblock.utilities.time.DateOrInstant
import com.spotify.scio.bigquery.BigQueryIO
import com.spotify.scio.io.CustomIO
import com.spotify.scio.testing.PipelineSpec
import org.joda.time.format.DateTimeFormat
import org.joda.time.{
  DateTimeZone => JodaDateTimeZone,
  Instant => JodaInstant,
  LocalDateTime => JodaLocalDateTime
}

class PushElationPatientEncountersTest extends PipelineSpec {
  implicit val environment: Environment = Environment.Test

  "PushElationPatientEncounters" should "transform and push PatientEncounters to GCS and BQ" in {
    val jt = JobTest[PushElationPatientEncounters.type]

    val medicalDataset: String = "medical"
    val patientEncountersTable = "patient_encounters"
    val patientEncountersToGcs = "Save Patient Encounters to GCS"

    val practices = mkPractice

    val serviceLocations = mkServiceLocation(practices.head)

    val users = mkUser(practices.head)

    val docTags = mkDocTag(practices.head)
    val (nonVisitNotes, nonVisitNoteBullets, nonVisitDocTags) =
      mKNonVisitNoteModels(practices.head, users.head, docTags.head)

    val (visitNotes, visitNoteBullets) = mkVisitNoteModels(practices.head, users.head)

    val (bills, billItems, billItemDxs, icd10s, icd9s) =
      mkBillModels(practices.head, serviceLocations.head, users.head, visitNotes.head)

    val expectedMessageId = "20200604000000"
    val expectedPatientEncounters =
      generatePatientEncounters(expectedPatient, expectedMessageId)

    jt.args(
        s"--environment=${environment.name}",
        s"--project=${environment.projectName}",
        s"--medicalDataset=$medicalDataset"
      )
      .input(BigQueryIO[Practice](Practice.queryAll), practices)
      .input(BigQueryIO[ServiceLocation](ServiceLocation.queryAll), serviceLocations)
      .input(BigQueryIO[User](User.queryAll), users)
      .input(BigQueryIO[DocumentTag](DocumentTag.queryAll), docTags)
      .input(BigQueryIO[NonVisitNote](NonVisitNote.queryAll), nonVisitNotes)
      .input(BigQueryIO[NonVisitNoteBullet](NonVisitNoteBullet.queryAll), nonVisitNoteBullets)
      .input(BigQueryIO[NonVisitNoteDocumentTag](NonVisitNoteDocumentTag.queryAll), nonVisitDocTags)
      .input(BigQueryIO[VisitNote](VisitNote.queryAll), visitNotes)
      .input(BigQueryIO[VisitNoteBullet](VisitNoteBullet.queryAll), visitNoteBullets)
      .input(BigQueryIO[Bill](Bill.queryAll), bills)
      .input(BigQueryIO[BillItem](BillItem.queryAll), billItems)
      .input(BigQueryIO[BillItemDx](BillItemDx.queryAll), billItemDxs)
      .input(BigQueryIO[Icd10](Icd10.queryAll), icd10s)
      .input(BigQueryIO[Icd9](Icd9.queryAll), icd9s)
      .input(ReadMrnPatients(Some("elation")),
             Seq(Patient(Some("DummyId"), "99", Datasource("elation"))))
      .output(
        BigQueryIO[PatientEncounter](
          s"${environment.projectName}:$medicalDataset.$patientEncountersTable"
        )
      )(results => {
        val encounters = results.map {
          _.copy(messageId = expectedMessageId, insertedAt = insertedAt)
        }
        encounters should containInAnyOrder(expectedPatientEncounters.flatten)
        encounters should haveSize(3)
      })
      .output(CustomIO[List[PatientEncounter]](patientEncountersToGcs))(
        results => {
          val encounters = results.map {
            _.map {
              _.copy(messageId = expectedMessageId, insertedAt = insertedAt)
            }
          }
          encounters.flatten should containInAnyOrder(expectedPatientEncounters.flatten)
          encounters should haveSize(1)
        }
      )
      .run()
  }
}

object PushElationPatientEncountersTest {
  private val dummyNum = 0
  private val externalPatientId = 99
  private val expectedPatient =
    Patient(Some("DummyId"), externalPatientId.toString, Datasource("elation"))
  private val insertedAt = Some(JodaInstant.ofEpochMilli(0))

  private def getLocalDateTime(day: String): JodaLocalDateTime = {
    val localDateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss")
    JodaLocalDateTime.parse(s"2020-05-${day}T00:00:00", localDateTimeFormatter)
  }

  private def localDateTimeToDateOrInstant(day: String): Option[DateOrInstant] = {
    val jodaTimeZoneID = "US/Pacific"
    val localDateTime = getLocalDateTime(day)
    val instant = localDateTime.toDateTime(JodaDateTimeZone.forID(jodaTimeZoneID)).toInstant
    Some(
      DateOrInstant(
        localDateTime.toString,
        Some(instant),
        None
      )
    )
  }

  private def mkPractice: Seq[Practice] =
    Seq(
      Practice(
        id = 1,
        name = "Practice",
        address_line1 = "76 Center Street",
        address_line2 = Some("3rd Floor"),
        city = "Waterbury",
        state = "CT",
        zip = "06702",
        phone = Some("123-456-7899"),
        fax = None
      )
    )

  private def mkServiceLocation(practice: Practice): Seq[ServiceLocation] =
    Seq(
      ServiceLocation(
        id = 2,
        address = "75 Center Street",
        suite = None,
        city = practice.city,
        state = practice.state,
        zip = practice.zip,
        phone = None,
        fax = None,
        practice_id = practice.id,
        is_primary = true,
        creation_time = Some(getLocalDateTime("01")),
        created_by_user_id = None,
        deleted_by_user_id = None,
        deletion_time = None
      )
    )

  private def mkUser(practice: Practice): Seq[User] = {
    val userId = 3
    Seq(
      User(
        id = userId,
        email = "user@email.com",
        is_active = true,
        practice_id = practice.id,
        first_name = Some("Gregory"),
        last_name = Some("House"),
        is_practice_admin = false,
        user_type = "",
        office_staff_id = None,
        specialty = Some("Emergency Medicine"),
        physician_id = None,
        credentials = Some("MD"),
        npi = Some("123")
      )
    )
  }

  private def mkDocTag(practice: Practice): Seq[DocumentTag] = {
    val docTagId = 4
    Seq(
      DocumentTag(
        id = docTagId,
        practice_id = Some(practice.id),
        value = "Doc tag value A",
        description = Some("Doc tag description A"),
        code_type = Some("CPT"),
        code = Some("Doc tag code A"),
        concept_name = None
      )
    )
  }

  private def mKNonVisitNoteModels(
    practice: Practice,
    user: User,
    docTag: DocumentTag
  ): (Seq[NonVisitNote], Seq[NonVisitNoteBullet], Seq[NonVisitNoteDocumentTag]) = {
    val nonVisitDate = getLocalDateTime("02")
    val nonVisitId = 5
    val nonVisitNote = NonVisitNote(
      id = nonVisitId,
      patient_id = externalPatientId,
      practice_id = practice.id,
      note_type = Some("phone"),
      document_date = nonVisitDate,
      chart_feed_date = nonVisitDate,
      last_modified = Some(nonVisitDate),
      creation_time = Some(nonVisitDate),
      created_by_user_id = Some(user.id),
      deletion_time = None,
      deleted_by_user_id = None,
      signed_by_user_id = Some(user.id),
      signed_time = Some(getLocalDateTime("03")),
      from_plr = 0
    )

    val nonVisitDocTag = NonVisitNoteDocumentTag(
      non_visit_note_id = nonVisitNote.id,
      document_tag_id = docTag.id
    )

    val bulletId = 6
    val nonVisitNoteBullet = NonVisitNoteBullet(
      id = bulletId,
      non_visit_note_id = nonVisitNote.id,
      sequence = dummyNum,
      text = "Non visit note A"
    )

    (Seq(nonVisitNote), Seq(nonVisitNoteBullet), Seq(nonVisitDocTag))
  }

  private def mkVisitNoteModels(
    practice: Practice,
    user: User
  ): (Seq[VisitNote], Seq[VisitNoteBullet]) = {
    val visitDate1 = getLocalDateTime("04")
    val visitNoteId1 = 11
    val visitNote1 = VisitNote(
      id = visitNoteId1,
      patient_id = externalPatientId,
      practice_id = practice.id,
      name = Some("Office Visit Note"),
      document_date = visitDate1,
      chart_feed_date = visitDate1,
      physician_user_id = Some(user.id),
      last_modified = Some(visitDate1),
      creation_time = Some(visitDate1),
      created_by_user_id = Some(user.id),
      deletion_time = None,
      deleted_by_user_id = None,
      signed_time = Some(getLocalDateTime("05")),
      signed_by_user_id = Some(user.id),
      from_plr = dummyNum,
      transition_of_care = None,
      meds_reconciled = None,
      current_meds_documented = None
    )

    val bulletId1A = 12
    val visitNoteBullet1A = VisitNoteBullet(
      id = bulletId1A,
      visit_note_id = visitNote1.id,
      parent_bullet_id = None,
      category = "Med",
      sequence = dummyNum,
      text = "Med Text A"
    )
    val bulletId1B = 122
    val visitNoteBullet1B = visitNoteBullet1A.copy(
      id = bulletId1B,
      text = "Med Text B",
      parent_bullet_id = Some(visitNoteBullet1A.id)
    )

    val visitDate2 = getLocalDateTime("14")
    val visitNoteId2 = 111
    val visitNote2 = visitNote1.copy(
      id = visitNoteId2,
      document_date = visitDate2,
      chart_feed_date = visitDate2,
      last_modified = Some(visitDate2),
      creation_time = Some(visitDate2),
      signed_time = None,
      signed_by_user_id = None
    )
    val bulletId2 = 1333
    val visitNoteBullet2 = visitNoteBullet1A.copy(
      id = bulletId2,
      visit_note_id = visitNote2.id,
      category = "Followup",
      text = "Followup Text A"
    )

    (Seq(visitNote1, visitNote2), Seq(visitNoteBullet1A, visitNoteBullet1B, visitNoteBullet2))
  }

  private def mkBillModels(
    practice: Practice,
    serviceLocation: ServiceLocation,
    user: User,
    visitNote: VisitNote
  ): (Seq[Bill], Seq[BillItem], Seq[BillItemDx], Seq[Icd10], Seq[Icd9]) = {
    val billId = 13
    val billDate = getLocalDateTime("06")
    val bill = Bill(
      id = billId,
      visit_note_id = visitNote.id,
      practice_id = practice.id,
      notes = None,
      billing_date = Some(billDate),
      ref_number = None,
      service_location_id = Some(serviceLocation.id),
      place_of_service = Some(12),
      billing_status = None,
      billing_error = None,
      show_dual_coding = false,
      copay_amount = None,
      copay_collection_date = None,
      referring_provider = None,
      referring_provider_state = None,
      deferred_bill = false,
      creation_time = Some(billDate),
      created_by_user_id = Some(user.id),
      last_modified = Some(billDate)
    )

    val billItemId = 15
    val billItem = BillItem(
      id = billItemId,
      bill_id = bill.id,
      cpt = "111F",
      seqno = dummyNum,
      last_modified = None,
      modifier_1 = None,
      modifier_2 = None,
      modifier_3 = None,
      modifier_4 = None,
      units = None,
      unit_charge = None,
      creation_time = None,
      created_by_user_id = None,
      deletion_time = None,
      deleted_by_user_id = None
    )

    val icd9Id = 9
    val icd10Id = 10
    val icd9 = Icd9(icd9Id, "icd9", "icd9")
    val icd10 = Icd10(icd10Id, "icd10", "icd10")

    val billItemDx = BillItemDx(
      bill_item_id = billItem.id,
      seqno = dummyNum,
      dx = "",
      icd10_id = icd10.id,
      icd9_id = Some(icd9.id)
    )

    (Seq(bill), Seq(billItem), Seq(billItemDx), Seq(icd10), Seq(icd9))
  }

  private def generatePatientEncounters(
    patient: Patient,
    messageId: String
  ): Seq[List[PatientEncounter]] = {
    val patientEncountersVisits =
      generatePatientEncountersVisits(patient, messageId)

    val patientEncountersNonVisits =
      generatePatientEncountersNonVisits(patient, messageId)

    Seq(patientEncountersVisits ::: patientEncountersNonVisits)
  }

  private def generatePatientEncountersVisits(
    patient: Patient,
    messageId: String
  ): List[PatientEncounter] = {
    val encounterReason = EncounterReason(
      code = None,
      codeSystem = None,
      codeSystemName = None,
      name = Some("Med"),
      notes = Some("Med Text A")
    )

    val encounterReasons1 = List(
      encounterReason,
      encounterReason.copy(notes = Some("Med Text B"))
    )

    val encounterReasons2 = List(
      encounterReason.copy(name = Some("Followup"), notes = Some("Followup Text A"))
    )

    val encounterIds1 = List(
      Identifier(Some("11"), Some("Visit Note")),
      Identifier(Some("13"), Some("Bill"))
    )

    val encounterIds2 = List(
      Identifier(Some("111"), Some("Visit Note"))
    )

    val npiId = Identifier(id = Some("123"), idType = Some("npi"))
    val emailId = Identifier(id = Some("user@email.com"), idType = Some("email"))
    val encounterProviderAddress = EncounterAdress(
      street1 = Some("76 Center Street"),
      street2 = Some("3rd Floor"),
      city = Some("Waterbury"),
      state = Some("CT"),
      zip = Some("06702")
    )

    val encounterProviders = List(
      EncounterProvider(
        identifiers = List(npiId, emailId),
        firstName = Some("Gregory"),
        lastName = Some("House"),
        credentials = List("MD"),
        address = Some(encounterProviderAddress),
        phone = Some("123-456-7899"),
        role = Some(EncounterProviderRole(None, None, None, name = Some("Emergency Medicine")))
      )
    )

    val encounterType1 = EncounterType(
      code = "111F",
      codeSystem = None,
      codeSystemName = Some("CPT"),
      name = Some("Office Visit Note")
    )

    val encounterType2 = EncounterType(
      code = "Unknown",
      codeSystem = None,
      codeSystemName = None,
      name = Some("Office Visit Note")
    )

    val encounterIcd10 = EncounterDiagnosis(
      code = Some("icd10"),
      codeSystem = None,
      codeSystemName = Some("ICD-10"),
      name = Some("icd10")
    )
    val encounterDxs1 = List(
      encounterIcd10.copy(code = Some("111F"), codeSystemName = Some("CPT"), name = None),
      encounterIcd10,
      encounterIcd10.copy(code = Some("icd9"), codeSystemName = Some("ICD-9"), name = Some("icd9"))
    )

    val encounterLocationAddressType1 = EncounterLocationAddressType(
      code = Some("12"),
      codeSystem = None,
      codeSystemName = Some("CMS Place of Service Code Set"),
      name = None
    )

    val encounterLocationAddress1 = EncounterAdress(
      street1 = Some("75 Center Street"),
      street2 = None,
      city = encounterProviderAddress.city,
      state = encounterProviderAddress.state,
      zip = encounterProviderAddress.zip
    )

    val encounterLocations1 = List(
      EncounterLocation(
        address = Some(encounterLocationAddress1),
        `type` = Some(encounterLocationAddressType1),
        name = None
      )
    )

    val encounter1 = Encounter(
      identifiers = encounterIds1,
      `type` = encounterType1,
      dateTime = localDateTimeToDateOrInstant("04"),
      endDateTime = localDateTimeToDateOrInstant("05"),
      providers = encounterProviders,
      locations = encounterLocations1,
      diagnoses = encounterDxs1,
      reasons = encounterReasons1,
      draft = false
    )

    val encounter2 = Encounter(
      identifiers = encounterIds2,
      `type` = encounterType2,
      dateTime = localDateTimeToDateOrInstant("14"),
      endDateTime = None,
      providers = encounterProviders,
      locations = List(),
      diagnoses = List(),
      reasons = encounterReasons2,
      draft = true
    )

    val patientEncounter = PatientEncounter(
      messageId = messageId,
      insertedAt = insertedAt,
      isStreaming = false,
      patient,
      encounter1
    )

    val patientEncountersVisits = List(
      patientEncounter,
      patientEncounter.copy(encounter = encounter2)
    )

    patientEncountersVisits
  }

  private def generatePatientEncountersNonVisits(
    patient: Patient,
    messageId: String
  ): List[PatientEncounter] = {
    val encounterType1 = EncounterType(
      code = "Unknown",
      codeSystem = None,
      codeSystemName = None,
      name = Some("phone")
    )

    val encounterIds1 =
      List(Identifier(Some("5"), Some("Non-Visit Note")))

    val encounterReason1A = EncounterReason(
      code = None,
      codeSystem = None,
      codeSystemName = None,
      name = None,
      notes = Some("Non visit note A")
    )

    val encounterReason1B = EncounterReason(
      code = Some("Doc tag code A"),
      codeSystem = None,
      codeSystemName = Some("CPT"),
      name = Some("Doc tag value A"),
      notes = Some("Doc tag description A")
    )

    val encounterReasons1 = List(encounterReason1A, encounterReason1B)

    val npiId = Identifier(id = Some("123"), idType = Some("npi"))
    val emailId = Identifier(id = Some("user@email.com"), idType = Some("email"))
    val encounterProviderAddress = EncounterAdress(
      street1 = Some("76 Center Street"),
      street2 = Some("3rd Floor"),
      city = Some("Waterbury"),
      state = Some("CT"),
      zip = Some("06702")
    )

    val encounterProviders = List(
      EncounterProvider(
        identifiers = List(npiId, emailId),
        firstName = Some("Gregory"),
        lastName = Some("House"),
        credentials = List("MD"),
        address = Some(encounterProviderAddress),
        phone = Some("123-456-7899"),
        role = Some(EncounterProviderRole(None, None, None, name = Some("Emergency Medicine")))
      )
    )

    val encounter1 = Encounter(
      identifiers = encounterIds1,
      `type` = encounterType1,
      dateTime = localDateTimeToDateOrInstant("02"),
      endDateTime = localDateTimeToDateOrInstant("03"),
      providers = encounterProviders,
      locations = List(),
      diagnoses = List(),
      reasons = encounterReasons1,
      draft = false
    )

    val patientEncounter = PatientEncounter(
      messageId = messageId,
      insertedAt = insertedAt,
      isStreaming = false,
      patient,
      encounter1
    )

    List(patientEncounter)
  }
}
