package cityblock.streaming.jobs

import cityblock.member.service.models.PatientInfo.{Datasource, Patient}
import cityblock.streaming.jobs.RedoxSubscriber.RedoxMessage
import cityblock.streaming.jobs.hie.Events.{Diagnosis, PatientHieEvent, Provider}
import cityblock.streaming.jobs.hie.HieMessagesHandler
import cityblock.utilities.time.DateOrInstant
import com.spotify.scio.testing._
import org.joda.time.{Instant, LocalDate}

import scala.io.Source

class HieMessagesHandlerTest extends PipelineSpec {
  "NoteMessagesHandler.parsePatientNotesMessages" should "parse patient notes, backfill long discharge DT & not overwrite admission DT" in {
    val patient1 = Patient(Some("abc123"), "12345", Datasource("ACPNY"))

    val sourceFile = Source.fromFile("src/test/resources/note.json")
    val noteMessage = sourceFile.getLines.mkString
    sourceFile.close

    val data =
      Seq(RedoxMessage(patient1, "Notes.New", noteMessage, Instant.now()))
    val instant = "2018-08-14T14:35:00.000Z"
    val dateOrInstant =
      DateOrInstant(instant, Some(Instant.parse(instant)), None)
    val dischargeInstant = "2018-07-02T19:10:00.000Z"
    val dischargeDateTime = "2018-07-02 15:10:00"
    val dischargeDate = "2018-07-02"
    val dischargeDateOrInstant =
      DateOrInstant(
        dischargeDateTime,
        Some(Instant.parse(dischargeInstant)),
        Some(LocalDate.parse(dischargeDate))
      )
    val expected = Seq(
      PatientHieEvent(
        messageId = "1234567",
        patient = patient1,
        source = "Healthix",
        eventDateTime = Some(dateOrInstant),
        dischargeDateTime = Some(dischargeDateOrInstant),
        fullText = Some(
          """John Snow (CITYBLOCK MRN: 19999abc-a999-111b-cb12-abd11111111)
            |was admitted to INTERBORO King's Landing (MRN: 9999999) -- Admission on 2018-08-10 14:35:00
            |-- Event type ED Admit and -- Discharged on 2018-07-02 15:10:00 --""".stripMargin
            .replaceAll("\n", " ")
        ),
        locationName = "NYU",
        fullLocationName = Some("INTERBORO King's Landing"),
        eventType = Some("Discharged"),
        visitType = None,
        visitNumber = Some("0123456"),
        diagnoses = List(
          Diagnosis(
            codeset = Some("ICD-10-CM"),
            code = Some("G000.0"),
            description = Some("A diagnosis")
          ),
          Diagnosis(
            codeset = Some("ICD-10-CM"),
            code = Some("F123.0"),
            description = Some("Other specified stuff")
          )
        ),
        dischargeDisposition = Some("06"),
        provider = None,
        admissionSource = None,
        facilityType = None,
        documentType = Some("Inpatient")
      )
    )

    runWithContext { sc =>
      val p = HieMessagesHandler.parsePatientNotesMessages(sc.parallelize(data))
      p should containInAnyOrder(expected)
    }
  }

  "NoteMessagesHandler.parsePatientNotesMessages" should "parse patient notes, backfill admission DT & not overwrite discharge DT" in {
    val patient1 = Patient(Some("abc123"), "12345", Datasource("ACPNY"))

    val sourceFile = Source.fromFile("src/test/resources/note2.json")
    val noteMessage = sourceFile.getLines.mkString
    sourceFile.close

    val data =
      Seq(RedoxMessage(patient1, "Notes.New", noteMessage, Instant.now()))
    val admissionDateTime = "2018-08-10 14:35:00"
    val instant = "2018-08-10T18:35:00.000Z"
    val admissionDate = "2018-08-10"
    val dateOrInstant =
      DateOrInstant(
        admissionDateTime,
        Some(Instant.parse(instant)),
        Some(LocalDate.parse(admissionDate))
      )
    val dischargeInstant = "2018-07-02T19:10:00.000Z"
    val dischargeDateOrInstant =
      DateOrInstant(
        dischargeInstant,
        Some(Instant.parse(dischargeInstant)),
        None
      )
    val expected = Seq(
      PatientHieEvent(
        messageId = "1234567",
        patient = patient1,
        source = "Healthix",
        eventDateTime = Some(dateOrInstant),
        dischargeDateTime = Some(dischargeDateOrInstant),
        fullText = Some(
          """John Snow (CITYBLOCK MRN: 19999abc-a999-111b-cb12-abd11111111) was admitted to Castle Black
            |(MRN: 9999999) -- Admission on 2018-08-10 14:35:00 -- Event type ED Admit and --
            |Discharged on 2018-07-02 15:10:00 --""".stripMargin
            .replaceAll("\n", " ")
        ),
        locationName = "NYU",
        fullLocationName = Some("Castle Black"),
        eventType = Some("Discharged"),
        visitType = Some("Inpatient"),
        visitNumber = Some("0123456"),
        diagnoses = List(
          Diagnosis(
            codeset = Some("ICD-10-CM"),
            code = Some("G000.0"),
            description = Some("A diagnosis")
          ),
          Diagnosis(
            codeset = Some("ICD-10-CM"),
            code = Some("F123.0"),
            description = Some("Other specified stuff")
          )
        ),
        dischargeDisposition = Some("06"),
        provider = None,
        admissionSource = None,
        facilityType = None,
        documentType = Some("Inpatient")
      )
    )

    runWithContext { sc =>
      val p = HieMessagesHandler.parsePatientNotesMessages(sc.parallelize(data))
      p should containInAnyOrder(expected)
    }
  }

  "NoteMessagesHandler.parsePatientNotesMessages" should "parse patient notes, no DTs and none to backfill" in {
    val patient1 = Patient(Some("abc123"), "12345", Datasource("ACPNY"))

    val sourceFile = Source.fromFile("src/test/resources/note3.json")
    val noteMessage = sourceFile.getLines.mkString
    sourceFile.close

    val data =
      Seq(RedoxMessage(patient1, "Notes.New", noteMessage, Instant.now()))
    val expected = Seq(
      PatientHieEvent(
        messageId = "1234567",
        patient = patient1,
        source = "Healthix",
        eventDateTime = None,
        dischargeDateTime = None,
        fullText = Some(
          "-- Admission on -- Event type ED Admit and -- Discharged on --"
        ),
        locationName = "NYU",
        fullLocationName = None,
        eventType = Some("Discharged"),
        visitType = Some("Outpatient"),
        visitNumber = Some("0123456"),
        diagnoses = List(
          Diagnosis(
            codeset = Some("ICD-10-CM"),
            code = Some("G000.0"),
            description = Some("A diagnosis")
          ),
          Diagnosis(
            codeset = Some("ICD-10-CM"),
            code = Some("F123.0"),
            description = Some("Other specified stuff")
          )
        ),
        dischargeDisposition = Some("06"),
        provider = None,
        admissionSource = None,
        facilityType = None,
        documentType = Some("Inpatient")
      )
    )

    runWithContext { sc =>
      val p = HieMessagesHandler.parsePatientNotesMessages(sc.parallelize(data))
      p should containInAnyOrder(expected)
    }
  }

  "NoteMessagesHandler.parsePatientAdminMessages" should "parse discharge patient admin messages" in {
    val patient1 = Patient(Some("abc123"), "12345", Datasource("ACPNY"))

    val sourceFile = Source.fromFile("src/test/resources/adminDischarge.json")
    val adminDischargeMessage = sourceFile.getLines.mkString
    sourceFile.close

    val data = Seq(
      RedoxMessage(
        patient1,
        "PatientAdmin.Discharge",
        adminDischargeMessage,
        Instant.now()
      )
    )
    val instant = "2019-01-01T08:22:00.000Z"
    val dateOrInstant =
      DateOrInstant(instant, Some(Instant.parse(instant)), None)
    val dischargeInstant = "2019-01-02T13:44:22.000Z"
    val dischargeDateOrInstant =
      DateOrInstant(
        dischargeInstant,
        Some(Instant.parse(dischargeInstant)),
        None
      )
    val expected = Seq(
      PatientHieEvent(
        messageId = "1234567",
        patient = patient1,
        source = "PatientPing",
        eventDateTime = Some(dateOrInstant),
        dischargeDateTime = Some(dischargeDateOrInstant),
        fullText = Some("This is a note about this patient"),
        locationName = "Bobcat Hospital",
        fullLocationName = None,
        eventType = Some("Discharge"),
        visitType = Some("Outpatient"),
        visitNumber = Some("01234567"),
        diagnoses = List(
          Diagnosis(
            codeset = Some("ICD-10"),
            code = Some("I123"),
            description = Some("Some Diag 1")
          ),
          Diagnosis(
            codeset = Some("ICD-10"),
            code = Some("I124"),
            description = Some("Some Diag 2")
          )
        ),
        dischargeDisposition = Some("Home"),
        provider = Some(
          Provider(
            npi = Some("1234567"),
            firstName = Some("Bobby"),
            lastName = Some("Fresh")
          )
        ),
        admissionSource = None,
        facilityType = None,
        documentType = None
      )
    )

    runWithContext { sc =>
      val p = HieMessagesHandler.parsePatientAdminMessages(sc.parallelize(data))
      p should containInAnyOrder(expected)
    }
  }

  "NoteMessagesHandler.parsePatientAdminMessages" should "parse arrival patient admin messages" in {
    val patient1 = Patient(Some("abc123"), "12345", Datasource("ACPNY"))

    val sourceFile = Source.fromFile("src/test/resources/adminArrival.json")
    val adminArrivalMessage = sourceFile.getLines.mkString
    sourceFile.close

    val data = Seq(
      RedoxMessage(
        patient1,
        "PatientAdmin.Arrival",
        adminArrivalMessage,
        Instant.now()
      )
    )
    val instant = "2019-01-01T08:22:00.000Z"
    val dateOrInstant =
      DateOrInstant(instant, Some(Instant.parse(instant)), None)
    val expected = Seq(
      PatientHieEvent(
        messageId = "1234567",
        patient = patient1,
        source = "PatientPing",
        eventDateTime = Some(dateOrInstant),
        dischargeDateTime = None,
        fullText = Some("This is a note about this patient"),
        locationName = "Bobcat Hospital",
        fullLocationName = None,
        eventType = Some("Admit"),
        visitType = Some("Emergency"),
        visitNumber = Some("01234567"),
        diagnoses = List(Diagnosis(codeset = None, code = None, description = None)),
        dischargeDisposition = None,
        provider = Some(
          Provider(
            npi = Some("1234567"),
            firstName = Some("Susan"),
            lastName = Some("Doctor")
          )
        ),
        admissionSource = Some("Physician Office"),
        facilityType = Some("SNF"),
        documentType = None
      )
    )

    runWithContext { sc =>
      val p = HieMessagesHandler.parsePatientAdminMessages(sc.parallelize(data))
      p should containInAnyOrder(expected)
    }
  }
}
