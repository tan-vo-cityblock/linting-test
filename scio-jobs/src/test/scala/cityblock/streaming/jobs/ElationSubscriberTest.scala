package cityblock.streaming.jobs

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, LocalDate => JavaLocalDate, LocalDateTime => JavaLocalDateTime}

import cityblock.ehrmodels.elation.datamodelapi.ccda.Ccda
import cityblock.member.service.models.PatientInfo.{Datasource, Patient}
import com.spotify.scio.testing.PipelineSpec
import cityblock.streaming.jobs.ElationSubscriber._
import cityblock.streaming.jobs.aggregations.Encounters._
import cityblock.streaming.jobs.aggregations.Medications._
import cityblock.streaming.jobs.aggregations.Problems._
import cityblock.streaming.jobs.aggregations.VitalSigns._
import cityblock.streaming.jobs.orders.Orders._
import cityblock.utilities.{Environment, SnomedCT}
import cityblock.utilities.backend.SttpHelper
import cityblock.utilities.time.DateOrInstant
import com.spotify.scio.bigquery.BigQueryIO
import com.spotify.scio.io._
import org.joda.time.{DateTimeZone, Instant => JodaInstant, LocalDate => JodaLocalDate}
import io.circe.Json
import io.circe.optics.JsonPath.root
import io.circe.parser.parse
import org.joda.time.format.{DateTimeFormat => JodaDateTimeFormat}
import org.scalatest.BeforeAndAfter
import org.scalatest.PrivateMethodTester._

import scala.io.{BufferedSource, Source}
import scala.util.Random

//TODO: Remove private method testing and abstract JobTest to enable multiple test sentences each with a JobTest.
class ElationSubscriberTest extends PipelineSpec with BeforeAndAfter {
  implicit val environment: Environment = Environment.Test

  private val mockPatientId: String = "151523299033089"
  private val expectedPatient =
    Patient(Some("DummyId"), mockPatientId, Datasource(datasource))

  "ElationSubscriber" should "successfully pull events from PubSub with attributes" in {
    val jt = JobTest[ElationSubscriber.type]

    val (mockCalendarEventPayload, attributes): (String, Map[String, String]) =
      constructPubsubFromJson(
        "src/test/resources/elationEvents/mockAppointmentHookResponse.json"
      )

    val eventType = s"${attributes("resource")}_${attributes("action")}"

    val messageId = s"${attributes("messageId")}"
    val insertedAt = JodaInstant.ofEpochMilli(0)

    val expectedElationPubSubMsg = Seq(
      ElationMessage(
        expectedPatient,
        eventType,
        messageId,
        mockCalendarEventPayload,
        insertedAt
      )
    )

    val expectedPubsubCalendarEvent = convertFileToString(
      "src/test/resources/elationEvents/mockAppointmentExpectedPubsub.json"
    )

    val expectedPubsubCalendarAttrib = Map(
      "type" -> "Cancel",
      "schedulingEventType" -> "Cancel",
      "topic" -> "scheduling",
      "hmac" -> "9f3e9d710c7b0eaf325278bddc812cfedbdccb5b79918e0abfc6bfb4317471f2"
    )

    val expectedPatientMeds =
      generatePatientMedications(expectedPatient, messageId)

    val subscription =
      s"projects/${environment.projectName}/subscriptions/elationEventsScioSub"
    val elationMessagesTable = "elation_messages"
    val patientProblemsTable = "patient_problems"
    val patientVitalSignsTable = "patient_vital_signs"
    val patientMedicationsTable = "patient_medications"
    val patientEncountersTable = "patient_encounters"
    val patientOrdersTable = "patient_orders"
    val medicalDataset: String = "medical"
    val patientProblemToGcs = "Save Patient Problems to GCS"
    val patientVitalSignsToGcs = "Save Patient Vital Signs to GCS"
    val schedulingTopic =
      s"projects/${environment.projectName}/topics/schedulingMessages"

    val expectedPatientProblemLists =
      generatePatientProblemLists(expectedPatient, messageId)

    val expectedPatientVitalSigns =
      generatePatientVitals(expectedPatient, messageId)

    val patientMedsToGcs = "Save Patient Medications to GCS"
    val patientEncountersToGcs = "Save Patient Encounters to GCS"

    jt.args(
        s"--subscription=$subscription",
        s"--environment=${environment.name}",
        s"--project=${environment.projectName}",
        s"--elationMessagesTable=$elationMessagesTable",
        s"--schedulingTopic=$schedulingTopic",
        s"--medicalDataset=$medicalDataset"
      )
      .input(
        PubsubIO[(String, Map[String, String])](subscription),
        Seq((mockCalendarEventPayload, attributes))
      )
      .output(
        BigQueryIO[ElationMessage](
          s"${environment.projectName}:streaming.$elationMessagesTable"
        )
      )(elationMsgs => {
        elationMsgs.map {
          _.copy(insertedAt = insertedAt)
        } should containInAnyOrder(expectedElationPubSubMsg)

        elationMsgs should haveSize(1)
      })
      .output(
        BigQueryIO[PatientProblem](
          s"${environment.projectName}:$medicalDataset.$patientProblemsTable"
        )
      )(problems => {
        problems should containInAnyOrder(expectedPatientProblemLists.flatten)
        problems should haveSize(5)
      })
      .output(CustomIO[List[PatientProblem]](patientProblemToGcs))(problems => {
        problems should containInAnyOrder(expectedPatientProblemLists)
        problems should haveSize(1)
      })
      .output(
        BigQueryIO[PatientVitalSign](
          s"${environment.projectName}:$medicalDataset.$patientVitalSignsTable"
        )
      )(vitals => {
        vitals should containInAnyOrder(expectedPatientVitalSigns.flatten)
        vitals should haveSize(10)
      })
      .output(CustomIO[List[PatientVitalSign]](patientVitalSignsToGcs))(
        vitals => {
          vitals should containInAnyOrder(expectedPatientVitalSigns)
          vitals should haveSize(1)
        }
      )
      .output(
        BigQueryIO[PatientMedication](
          s"${environment.projectName}:$medicalDataset.$patientMedicationsTable"
        )
      )(meds => {
        meds should containInAnyOrder(expectedPatientMeds.flatten)
        meds should haveSize(5)
      })
      .output(CustomIO[List[PatientMedication]](patientMedsToGcs))(meds => {
        meds should containInAnyOrder(expectedPatientMeds)
        meds should haveSize(1)
      })
      .output(
        BigQueryIO[PatientEncounter](
          s"${environment.projectName}:$medicalDataset.$patientEncountersTable"
        )
      )(encounters => {
        encounters should haveSize(0)
      })
      .output(CustomIO[List[PatientEncounter]](patientEncountersToGcs))(
        encounters => {
          encounters should haveSize(0)
        }
      )
      .output(PubsubIO[(String, Map[String, String])](schedulingTopic))(
        events => {
          events should containInAnyOrder(
            Seq((expectedPubsubCalendarEvent, expectedPubsubCalendarAttrib))
          )
          events should haveSize(1)
        }
      )
      .output(
        BigQueryIO[PatientOrder](
          s"${environment.projectName}:$medicalDataset.$patientOrdersTable"
        )
      )(orders => {
        orders should beEmpty
      })
      .run()
  }

  "ElationSubscriber" should "not generate PatientMedications for CCDA with no Medications" in {
    val ccdaStrNoMeds = convertFileToString(
      "src/test/resources/elationEvents/mockCcdaPullResponseNoHealthData.json"
    )
    val ccdaNoMeds = Ccda(ccdaStrNoMeds) match {
      case Right(ccda) => ccda
      case Left(error) =>
        throw new Exception(
          s"Could not init mock ccda with no meds from str $error"
        )
    }

    runWithContext { sc =>
      val elationCcdaToPatientMeds =
        PrivateMethod[List[PatientMedication]]('elationCcdaToPatientMeds)
      val result = ElationSubscriber invokePrivate elationCcdaToPatientMeds(
        "dumMessageId",
        expectedPatient,
        Option(ccdaNoMeds)
      )
      sc.parallelize(result) should beEmpty
    }
  }

  "Elation Subscriber" should "generate PatientOrders for Elation Lab Hook Response" in {
    val labResponse = Seq(
      constructPubsubFromJson(
        "src/test/resources/elationEvents/mockLabHookResponse.json"
      )
    )

    val expectedResult = generateExpectedOrderResult()

    runWithContext { sc =>
      val pubsubInput = sc.parallelize(labResponse)
      val elationMsgsAndObjs = processElationPubSubMessages(pubsubInput, testing = true)
      val elationLabs =
        processElationLabs(elationMsgsAndObjs, testing = true)

      elationLabs should containInAnyOrder(Seq(expectedResult))
    }
  }

  def constructPubsubFromJson(
    filePath: String
  ): (String, Map[String, String]) = {
    val mockResponse: String = convertFileToString(filePath)
    val mockJson: Json =
      parse(mockResponse).getOrElse(Json.Null)

    val messageIdPath = root.event_id.long
    val messageId = messageIdPath.getOption(mockJson) match {
      case Some(messageIdL) => messageIdL.toString
      case _                => Random.alphanumeric.take(10).mkString
    }
    val resourcePath = root.resource.string
    val actionPath = root.action.string

    (
      mockResponse,
      Map(
        "messageId" -> messageId,
        "resource" -> resourcePath.getOption(mockJson).getOrElse(""),
        "action" -> actionPath.getOption(mockJson).getOrElse("")
      )
    )
  }

  private def convertFileToString(path: String): String = {
    val file: BufferedSource = Source.fromFile(path)
    val str = file.getLines.mkString
    file.close
    str
  }

  private def localDateToDateOrInstant(
    localDateStr: String
  ): Option[DateOrInstant] = {
    val formatter: DateTimeFormatter =
      DateTimeFormatter.ofPattern("uuuu-MM-dd")

    val javaLocalDate = JavaLocalDate.parse(localDateStr, formatter)
    val localDateEpoch = javaLocalDate.atStartOfDay
      .atZone(ZoneOffset.UTC)
      .toInstant
      .toEpochMilli

    Some(
      DateOrInstant(
        javaLocalDate.toString,
        None,
        Some(new JodaLocalDate(localDateEpoch, DateTimeZone.UTC))
      )
    )
  }

  private def localDateTimeToDateOrInstant(
    localDateTimeStr: String
  ): Option[DateOrInstant] = {
    val formatter: DateTimeFormatter =
      DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ssX")

    val javaLocalDateTime = JavaLocalDateTime.parse(localDateTimeStr, formatter)
    val epochMilli =
      javaLocalDateTime.atZone(ZoneOffset.UTC).toInstant.toEpochMilli

    Some(
      DateOrInstant(
        localDateTimeStr,
        Some(JodaInstant.ofEpochMilli(epochMilli)),
        None
      )
    )
  }

  private def generatePatientProblemLists(
    patient: Patient,
    messageId: String
  ): Seq[List[PatientProblem]] = {
    val problemCategory =
      ProblemCategory(
        SnomedCT.ProblemType.Complaint.code,
        Some(SnomedCT.codeSystem),
        Some(SnomedCT.codeSystemName),
        Some(SnomedCT.ProblemType.Complaint.name)
      )

    val problemStatusActive = ProblemStatus(
      Some(SnomedCT.ProblemStatus.Active.code),
      Some(SnomedCT.codeSystem),
      Some(SnomedCT.codeSystemName),
      Some(SnomedCT.ProblemStatus.Active.name)
    )

    val problemStatusInactive = ProblemStatus(
      Some(SnomedCT.ProblemStatus.Inactive.code),
      Some(SnomedCT.codeSystem),
      Some(SnomedCT.codeSystemName),
      Some(SnomedCT.ProblemStatus.Inactive.name)
    )

    val problemStatusResolved = ProblemStatus(
      Some(SnomedCT.ProblemStatus.Resolved.code),
      Some(SnomedCT.codeSystem),
      Some(SnomedCT.codeSystemName),
      Some(SnomedCT.ProblemStatus.Resolved.name)
    )

    val problemHealthStatus = ProblemHealthStatus(None, None, None, None)

    val problem1A = Problem(
      localDateToDateOrInstant("2019-03-26"),
      None,
      "23406007",
      Some(SnomedCT.codeSystem),
      Some(SnomedCT.codeSystemName),
      Some("Broken arm and finger"),
      problemCategory,
      problemHealthStatus,
      problemStatusActive
    )

    val problem1B = problem1A.copy(Code = "18171007")

    val problem2 = problem1A.copy(
      StartDate = localDateToDateOrInstant("2019-03-14"),
      EndDate = localDateToDateOrInstant("2019-03-27"),
      Code = "6142004",
      Name = Some("Flu"),
      Status = problemStatusResolved
    )

    val problem3A = problem1A.copy(
      StartDate = localDateToDateOrInstant("2019-03-26"),
      Code = "73211009",
      Name = Some("Diabetes and Obese"),
      Status = problemStatusInactive
    )

    val problem3B = problem3A.copy(Code = "414916001")

    Seq(
      List(
        PatientProblem(messageId, patient, problem1A),
        PatientProblem(messageId, patient, problem1B),
        PatientProblem(messageId, patient, problem2),
        PatientProblem(messageId, patient, problem3A),
        PatientProblem(messageId, patient, problem3B)
      )
    )
  }

  private def generatePatientVitals(
    patient: Patient,
    messageId: String
  ): Seq[List[PatientVitalSign]] = {
    val bmi = VitalSign(
      "39156-5",
      "2.16.840.1.113883.6.1",
      "LOINC",
      "Body mass index",
      "completed",
      "",
      localDateTimeToDateOrInstant("2019-03-18T01:11:10Z"),
      "21.52",
      "kg/m2"
    )

    val height = bmi.copy(
      Code = "8302-2",
      Name = "Body height",
      Value = "177.800",
      Units = "cm"
    )

    val weight1 = bmi.copy(
      Code = "29463-7",
      Name = "Body weight",
      Value = "68.039",
      Units = "kg"
    )

    val oxygen1 = bmi.copy(
      Code = "59408-5",
      Name = "Oxygen saturation",
      Value = "98",
      Units = "%"
    )

    val respRate = bmi.copy(
      Code = "9279-1",
      Name = "Respiratory rate",
      Value = "20",
      Units = "/min"
    )

    val heartRate1 = bmi.copy(
      Code = "8867-4",
      Name = "Heart rate",
      Value = "80",
      Units = "/min"
    )

    val headCircum = bmi.copy(
      Code = "8287-5",
      Name = "Head circumference",
      Value = "40",
      Units = "cm"
    )

    val temperature1 = bmi.copy(
      Code = "8310-5",
      Name = "Body temperature",
      Value = "36.667",
      Units = "Cel"
    )

    val bpSys = bmi.copy(
      Code = "8480-6",
      Name = "Systolic blood pressure",
      Value = "120",
      Units = "mm[Hg]"
    )

    val bpDias = bpSys.copy(
      Code = "8462-4",
      Name = "Diastolic blood pressure",
      Value = "80"
    )

    val patientVitalSign =
      PatientVitalSign(messageId, patient, bmi)

    Seq(
      List(
        patientVitalSign,
        patientVitalSign.copy(vitalSign = height),
        patientVitalSign.copy(vitalSign = weight1),
        patientVitalSign.copy(vitalSign = oxygen1),
        patientVitalSign.copy(vitalSign = respRate),
        patientVitalSign.copy(vitalSign = heartRate1),
        patientVitalSign.copy(vitalSign = headCircum),
        patientVitalSign.copy(vitalSign = temperature1),
        patientVitalSign.copy(vitalSign = bpSys),
        patientVitalSign.copy(vitalSign = bpDias)
      )
    )
  }

  private def generatePatientMedications(
    patient: Patient,
    messageId: String
  ): Seq[List[PatientMedication]] = {
    val ccdaDateFormat = JodaDateTimeFormat.forPattern("yyyyMMdd")
    def makeDate(raw: String): Option[DateOrInstant] =
      Some(
        DateOrInstant(raw, None, Some(JodaLocalDate.parse(raw, ccdaDateFormat)))
      )

    val unknown = "Unknown"
    val freeText = "Free Text"
    val medRate = MedicationRate(None, None)
    val medRoute = MedicationRoute(unknown, unknown, unknown, unknown)

    val medProduct1 = MedicationProduct(
      "311571",
      "2.16.840.1.113883.6.88",
      "RXNORM",
      "MetFORMIN ER 500 mg Tab ER 24hr"
    )
    val medProduct2 =
      medProduct1.copy(Code = "1364435", Name = "Eliquis 2.5 mg Tab")

    val medProduct3 =
      medProduct1.copy(Code = "665033", Name = "Januvia 100 mg Tab")

    val medProduct4 =
      medProduct1.copy(Code = "310965", Name = "Ibuprofen 200 mg Tab")

    val medProduct5 =
      medProduct1.copy(Code = "308182", Name = "Amoxicillin 250 mg Cap")

    val medDose1 = MedicationDose("500", "mg")
    val medDose2 = MedicationDose("2.5", "mg")
    val medDose3 = MedicationDose("100", "mg")
    val medDose4 = MedicationDose("200", "mg")
    val medDose5 = MedicationDose("250", "mg")

    val medFreq1 =
      MedicationFrequency(
        "1 tablet orally 2 times per day with meals",
        freeText
      )
    val medFreq2 = MedicationFrequency(unknown, unknown)
    val medFreq3 = medFreq1.copy(Period = "1 tablet orally daily")
    val medFreq4 = medFreq2
    val medFreq5 = medFreq2

    val dateStrt1 = makeDate("20190213")
    val dateEnd1 = makeDate("20190313")
    val dateStrt2 = makeDate("20190213")
    val dateStrt3 = makeDate("20190223")
    val dateStrt4 = makeDate("20190423")
    val dateStrt5 = makeDate("20190423")

    val med1 = Medication(
      Prescription = false,
      FreeTextSig = None,
      Dose = medDose1,
      Rate = medRate,
      Route = medRoute,
      StartDate = dateStrt1,
      EndDate = dateEnd1,
      Frequency = medFreq1,
      IsPRN = None,
      Product = medProduct1
    )

    val med2 = med1.copy(
      Dose = medDose2,
      StartDate = dateStrt2,
      EndDate = None,
      Frequency = medFreq2,
      Product = medProduct2
    )

    val med3 = med2.copy(
      Dose = medDose3,
      StartDate = dateStrt3,
      Frequency = medFreq3,
      Product = medProduct3
    )

    val med4 = med2.copy(
      Dose = medDose4,
      StartDate = dateStrt4,
      Frequency = medFreq4,
      Product = medProduct4
    )

    val med5 = med2.copy(
      Dose = medDose5,
      StartDate = dateStrt5,
      Frequency = medFreq5,
      Product = medProduct5
    )

    val patientMed = PatientMedication(messageId, patient, med1)
    Seq(
      List(
        patientMed,
        patientMed.copy(medication = med2),
        patientMed.copy(medication = med3),
        patientMed.copy(medication = med4),
        patientMed.copy(medication = med5)
      )
    )
  }

  private def generateExpectedOrderResult(): PatientOrder =
    PatientOrder(
      "4667986",
      Patient(Some("DummyId"), "151523299033089", Datasource("elation", None)),
      Order(
        "91143209000",
        None,
        Some(
          DateOrInstant(
            "2019-04-18T18:24:33Z",
            Some(org.joda.time.Instant.parse("2019-04-18T18:24:33.000Z")),
            None
          )
        ),
        Some(
          DateOrInstant(
            "2019-04-18T18:24:00Z",
            Some(org.joda.time.Instant.parse("2019-04-18T18:24:00.000Z")),
            None
          )
        ),
        Some(
          DateOrInstant(
            "2019-04-18T18:24:35Z",
            Some(org.joda.time.Instant.parse("2019-04-18T18:24:35.000Z")),
            None
          )
        ),
        List[String](),
        None,
        None,
        Some(
          Provider(
            Some("1090078601"),
            Some("150679616754852"),
            Some("ElationId"),
            Some("Gregory"),
            Some("House"),
            List(),
            None,
            None,
            None
          )
        ),
        List(),
        "FINAL",
        None,
        None,
        List(
          Result(
            Code = Some("5902-2"),
            Codeset = None,
            Description = Some("Prothrombin Time"),
            Specimen = None,
            Value = "10",
            ValueType = "Numeric",
            CompletionDateTime = Some(
              DateOrInstant(
                "2019-04-18T18:24:35Z",
                Some(org.joda.time.Instant.parse("2019-04-18T18:24:35.000Z")),
                None
              )
            ),
            FileType = None,
            Units = Some("sec"),
            Notes = List(),
            AbnormalFlag = None,
            Status = "FINAL",
            Producer = None,
            Performer = None,
            ReferenceRange = Some(ResultReferenceRange(Some("9.1"), Some("12"), None)),
            ObservationMethod = None
          ),
          Result(
            Code = Some("6301-6"),
            Codeset = None,
            Description = Some("INR"),
            Specimen = None,
            Value = "1.0",
            ValueType = "Numeric",
            CompletionDateTime = Some(
              DateOrInstant(
                "2019-04-18T18:24:35Z",
                Some(org.joda.time.Instant.parse("2019-04-18T18:24:35.000Z")),
                None
              )
            ),
            FileType = None,
            Units = None,
            Notes = List(),
            AbnormalFlag = None,
            Status = "FINAL",
            Producer = None,
            Performer = None,
            ReferenceRange = Some(ResultReferenceRange(Some("0.1"), Some("1.2"), None)),
            ObservationMethod = None
          )
        )
      )
    )
}
