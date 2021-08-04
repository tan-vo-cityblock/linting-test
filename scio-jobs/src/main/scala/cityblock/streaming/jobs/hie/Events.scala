package cityblock.streaming.jobs.hie

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.streaming.jobs.common._
import cityblock.streaming.jobs.hie.HieMessagesHandler._
import cityblock.streaming.jobs.notes.Notes.{Component, Note}
import cityblock.utilities.Loggable
import cityblock.utilities.time.DateOrInstant
import com.spotify.scio.bigquery.types.BigQueryType

object Events {
  @BigQueryType.toTable
  case class Diagnosis(codeset: Option[String], code: Option[String], description: Option[String])

  @BigQueryType.toTable
  case class Provider(npi: Option[String], firstName: Option[String], lastName: Option[String])

  @BigQueryType.toTable
  case class PatientHieEvent(
    messageId: String,
    patient: Patient,
    source: String,
    eventDateTime: Option[DateOrInstant],
    dischargeDateTime: Option[DateOrInstant],
    fullText: Option[String],
    locationName: String,
    fullLocationName: Option[String],
    eventType: Option[String],
    documentType: Option[String],
    visitType: Option[String],
    visitNumber: Option[String],
    diagnoses: List[Diagnosis],
    dischargeDisposition: Option[String],
    provider: Option[Provider],
    facilityType: Option[String],
    admissionSource: Option[String]
  )

  object PatientHieEvent extends ParsableData with Loggable {
    def fromParsedNoteMessage(
      parsedNoteMessage: ParsedNoteMessage
    ): PatientHieEvent = {
      val rawNote = parsedNoteMessage.rawNote
      val rawVisit = rawNote.Visit
      val note = rawNote.Note
      val components = note.Components
      val extensions = rawVisit.Extensions
      var visitDischargeDateTime: Option[DateOrInstant] = None
      val backfilledEventDateTime = rawVisit.VisitDateTime.orElse(
        backfillDate(note.FileContents, "Admission on")
      )

      for { ext <- extensions } yield {
        val dischargeDateTimeObj = ext.`discharge-date-time`

        for { dateTimeValue <- dischargeDateTimeObj } yield {
          visitDischargeDateTime = dateTimeValue.dateTime
        }
      }
      val backfilledDischargeDateTime = visitDischargeDateTime.orElse(
        backfillDate(note.FileContents, "Discharged on")
      )

      PatientHieEvent(
        messageId = parsedNoteMessage.messageId,
        patient = parsedNoteMessage.message.patient,
        source = "Healthix", // TODO: Don't hardcode this
        eventDateTime = backfilledEventDateTime,
        dischargeDateTime = backfilledDischargeDateTime,
        fullText = note.FileContents,
        locationName = note.DocumentID,
        fullLocationName = note.FileContents
          .map(backfillFullLocationName(_): String)
          .filter(_.trim.nonEmpty),
        eventType = getComponentValue("Event Type", components),
        documentType = Some(canonicalizeVisitType(note.DocumentType, parsedNoteMessage.messageId)),
        visitType = getComponentValue("Visit Type", components)
          .map(canonicalizeVisitType(_, parsedNoteMessage.messageId)),
        visitNumber = rawVisit.VisitNumber,
        diagnoses = getDiagnoses(note),
        dischargeDisposition = getComponentValue("dischargeDisposition", components),
        provider = None,
        facilityType = None,
        admissionSource = None
      )
    }

    def fromParsedPatientAdminMessage(
      parsedAdminMessage: ParsedAdminMessage
    ): PatientHieEvent = {
      val rawAdminMessage = parsedAdminMessage.rawAdminMessage
      val rawVisit = rawAdminMessage.Visit
      val rawPatient = rawAdminMessage.Patient
      val rawMeta = rawAdminMessage.Meta

      val formattedNotes = rawPatient.Notes.mkString("\n")
      val formattedDiagnoses = rawPatient.Diagnoses.map(patientDiagnosis => {
        Diagnosis(
          codeset = patientDiagnosis.Codeset,
          code = patientDiagnosis.Code,
          description = patientDiagnosis.Name
        )
      })
      val attendingProvider = rawVisit.AttendingProvider
      val formattedProvider = Provider(
        npi = attendingProvider.ID,
        firstName = attendingProvider.FirstName,
        lastName = attendingProvider.LastName
      )
      // https://cityblock.kanbanize.com/ctrl_board/2/cards/2590/details
      val formattedEventType = rawAdminMessage.Meta.EventType match {
        case "Discharge" => "Discharge"
        case "Arrival"   => "Admit"
        case _           => "Unknown"
      }
      val formattedDischargeDisposition =
        (for { dischargeDisposition <- rawVisit.DischargeStatus } yield {
          dischargeDisposition.Description
        }).flatten
      val extensions = rawVisit.Extensions
      var visitAdmissionSource: Option[String] = None
      val source = rawAdminMessage.Meta.Source.Name match {
        case Some("PatientPing Source (p)")      => "PatientPing"
        case Some("PatientPing Source (s)")      => "PatientPing"
        case Some("CRISP [PROD] ADT Source (p)") => "CRISP"
        case _                                   => "Unknown"
      }

      for { ext <- extensions } yield {
        val admissionSourceObj = ext.`admission-source`

        for { admissionSourceValue <- admissionSourceObj } yield {
          visitAdmissionSource = admissionSourceValue.codeableConcept.text
        }
      }

      PatientHieEvent(
        messageId = parsedAdminMessage.messageId,
        patient = parsedAdminMessage.message.patient,
        source = source,
        eventDateTime = rawVisit.VisitDateTime,
        dischargeDateTime = rawVisit.DischargeDateTime,
        fullText = Some(formattedNotes),
        locationName = rawMeta.FacilityCode.getOrElse("Unknown Facility"),
        fullLocationName = None,
        eventType = Some(formattedEventType),
        documentType = None,
        visitType = rawVisit.PatientClass
          .map(canonicalizeVisitType(_, parsedAdminMessage.messageId)),
        visitNumber = rawVisit.AccountNumber, // This is where Redox is mapping PV1.50, which is where PatientPing shoves their generated ID
        diagnoses = formattedDiagnoses,
        dischargeDisposition = formattedDischargeDisposition,
        provider = Some(formattedProvider),
        facilityType = rawVisit.AttendingProvider.Location.Type,
        admissionSource = visitAdmissionSource
      )
    }

    def getComponentValue(id: String, components: List[Component]): Option[String] = {
      val component =
        components.find(component => component.ID.getOrElse("") == id)

      component match {
        case Some(c) => c.Value
        case _       => None
      }
    }

    def getDiagnoses(note: Note): List[Diagnosis] = {
      val diagnosisComponents = note.Components.filter(
        component => component.ID.getOrElse("") == "diagnosis"
      )

      for {
        component <- diagnosisComponents
        compValue <- component.Value
      } yield {
        val splitValue = compValue.split('^').lift
        val code = splitValue(0)
        val description = splitValue(1)
        val codeset = splitValue(2)
        Diagnosis(codeset, code, description)
      }
    }

    private def canonicalizeVisitType(visitType: String, messageId: String): String =
      visitType match {
        case "IA" | "Inpatient" => "Inpatient"
        case "O" | "Outpatient" => "Outpatient"
        case "ED" | "Emergency" => "Emergency"
        case "Discharge"        => "Discharge"
        case "Arrival"          => "Admit"
        case "G"                => "General"
        case "S"                => "Silent"
        case _ =>
          logger.info(
            s"""$messageId with $visitType was converted to "Unknown""""
          )
          "Unknown"
      }

    private def backfillFullLocationName(fullText: String): String = {
      val facilityName =
        raw""".*?\) +was (?:admitted to|discharged from) (.*) +\(.*""".r
      fullText match {
        case facilityName(backfilledFacilityName) =>
          backfilledFacilityName.trim
        case _ => ""
      }
    }

    private def backfillDate(
      fullText: Option[String],
      fullTextSearchTerm: String
    ): Option[DateOrInstant] = {
      val canonicalizedSearchTerm = fullTextSearchTerm.toLowerCase
      val freeTextSearchTerm =
        raw"""(?i).*?$canonicalizedSearchTerm (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}|[a-z ]+[0-9 ]+:[0-9 ]+[pa]m) *--.*""".r
      fullText.getOrElse("") match {
        case freeTextSearchTerm(backfilledPayloadDate) =>
          parseDateOrInstant(Option(backfilledPayloadDate))
        case _ => None
      }
    }
  }
}
