package cityblock.aggregators.jobs

import cityblock.member.service.models.PatientInfo.{Datasource, Patient}
import cityblock.streaming.jobs.RedoxSubscriber.RedoxMessage
import cityblock.streaming.jobs.aggregations.Encounters._
import cityblock.streaming.jobs.aggregations.Medications._
import cityblock.streaming.jobs.aggregations.Problems._
import cityblock.streaming.jobs.aggregations.Results._
import cityblock.streaming.jobs.aggregations.VitalSigns._
import cityblock.utilities.Environment
import cityblock.utilities.time.DateOrInstant
import com.spotify.scio.testing._
import org.joda.time.{Instant, LocalDate}

import scala.io.{Codec, Source}

class ClinicalSummaryRefresherTest extends PipelineSpec {
  implicit val environment: Environment = Environment.Test
  private val insertedAt = Some(Instant.ofEpochMilli(0))
  private val isStreaming = true
  "ClinicalSummaryRefresher.parsePatientMedicalData" should "parse patient problem list" in {
    val patient1 = Patient(Some("abc123"), "12345", Datasource("ACPNY"))
    val sourceFile =
      Source.fromFile("src/test/resources/ccd1.json")(Codec("UTF-8"))
    val patientQueryResponse = sourceFile.getLines.mkString
    sourceFile.close

    val data = Seq(
      RedoxMessage(patient1, "PatientAdmin.Discharge", patientQueryResponse, Instant.now())
    )
    val localDate = "2017-10-23"
    val dateOrInstant =
      DateOrInstant(localDate, None, Some(LocalDate.parse(localDate)))
    val problem =
      Problem(
        Some(dateOrInstant),
        None,
        "15640441000119104",
        Some("2.16.840.1.113883.6.96"),
        Some("SNOMED CT"),
        Some("Primary open angle glaucoma of right eye"),
        ProblemCategory(
          "55607006",
          Some("2.16.840.1.113883.6.96"),
          Some("SNOMED CT"),
          Some("Problem")
        ),
        ProblemHealthStatus(Some(""), Some(""), Some(""), Some("")),
        ProblemStatus(
          Some("55561003"),
          Some("2.16.840.1.113883.6.96"),
          Some("SNOMED CT"),
          Some("Active")
        )
      )
    val expected = Seq(
      List(
        PatientProblem(
          "337753451",
          patient1,
          problem
        ),
        PatientProblem(
          "337753451",
          patient1,
          problem.copy(Code = "426875007", Name = Some("Diabetes 1.5, managed as type 2 (HCC)"))
        ),
        PatientProblem(
          "337753451",
          patient1,
          problem.copy(Code = "16001004", Name = Some("Ear ache"))
        ),
        PatientProblem(
          "337753451",
          patient1,
          problem.copy(Code = "268650001", Name = Some("Nervous gastritis"))
        )
      )
    )

    runWithContext { sc =>
      val p =
        ClinicalSummaryRefresher
          .parsePatientMedicalData(sc.parallelize(data), isStreaming = true)
      p.patientProblemLists should containInAnyOrder(expected)
    }
  }

  "ClinicalSummaryRefresher.parsePatientMedicalData" should "parse patient medication list" in {
    val patient1 = Patient(Some("abc123"), "12345", Datasource("ACPNY"))

    val sourceFile =
      Source.fromFile("src/test/resources/ccd2.json")(Codec("UTF-8"))
    val patientQueryResponse = sourceFile.getLines.mkString
    sourceFile.close

    val data = Seq(
      RedoxMessage(patient1, "PatientAdmin.Discharge", patientQueryResponse, Instant.now())
    )
    val instant1 = "2016-08-17T19:15:00.000Z"
    val instant2 = "2014-09-10T17:00:00.000Z"
    val dateOrInstant1 =
      DateOrInstant(instant1, Some(Instant.parse(instant1)), None)
    val dateOrInstant2 =
      DateOrInstant(instant2, Some(Instant.parse(instant2)), None)
    val expected = Seq(
      List(
        PatientMedication(
          "337753451",
          patient1,
          Medication(
            Prescription = true,
            Some(""),
            MedicationDose("5", "MG"),
            MedicationRate(Some(""), Some("")),
            MedicationRoute("PO", "2.16.840.1.113883.3.26.1.1", "NCI Thesaurus", "Oral"),
            None,
            None,
            MedicationFrequency("", ""),
            Some(false),
            MedicationProduct(
              "1049621",
              "2.16.840.1.113883.6.88",
              "RxNorm",
              "oxyCODONE (ROXICODONE) 5 MG immediate release tablet"
            )
          )
        ),
        PatientMedication(
          "337753451",
          patient1,
          Medication(
            Prescription = true,
            Some(""),
            MedicationDose("5", "MG"),
            MedicationRate(Some(""), Some("")),
            MedicationRoute(
              "NEBULIZATION",
              "2.16.840.1.113883.3.26.1.1",
              "NCI Thesaurus",
              "Nebulization"
            ),
            Some(dateOrInstant1),
            None,
            MedicationFrequency("", ""),
            Some(false),
            MedicationProduct(
              "245314",
              "2.16.840.1.113883.6.88",
              "RxNorm",
              "albuterol (PROVENTIL) nebulizer solution 5 mg"
            )
          )
        ),
        PatientMedication(
          "337753451",
          patient1,
          Medication(
            Prescription = true,
            Some(""),
            MedicationDose("1", "g"),
            MedicationRate(Some(""), Some("")),
            MedicationRoute("IM", "2.16.840.1.113883.3.26.1.1", "NCI Thesaurus", "Intramuscular"),
            Some(dateOrInstant2),
            None,
            MedicationFrequency("", ""),
            Some(false),
            MedicationProduct(
              "313819",
              "2.16.840.1.113883.6.88",
              "RxNorm",
              "ampicillin (OMNIPEN) injection 1 g"
            )
          )
        )
      )
    )

    runWithContext { sc =>
      val p =
        ClinicalSummaryRefresher
          .parsePatientMedicalData(sc.parallelize(data), isStreaming = true)
      p.patientMedicationLists should containInAnyOrder(expected)
    }
  }

  "ClinicalSummaryRefresher.parsePatientMedicalData" should "parse patient encounter list" in {
    val patient1 = Patient(Some("abc123"), "12345", Datasource("ACPNY"))

    val sourceFile =
      Source.fromFile("src/test/resources/ccd3.json")(Codec("UTF-8"))
    val patientQueryResponse = sourceFile.getLines.mkString
    sourceFile.close

    val data = Seq(
      RedoxMessage(patient1, "PatientAdmin.Discharge", patientQueryResponse, Instant.now())
    )
    val localDate2 = "2018-04-19"
    val localDate1 = "2018-04-12"
    val dateOrInstant2 =
      DateOrInstant(localDate2, None, Some(LocalDate.parse(localDate2)))
    val dateOrInstant1 = {
      DateOrInstant(localDate1, None, Some(LocalDate.parse(localDate1)))
    }
    val providers = List(
      EncounterProvider(
        List(),
        Some("Physician"),
        Some("Family Medicine"),
        List(),
        Some(Address(
          Some("123 Anywhere Street"),
          None,
          Some("MADISON"),
          Some("WI"),
          Some("53711")
        )),
        Some("+15555555555"),
        Some(EncounterProviderRole(Some(""), Some(""), Some(""), Some("Family Medicine")))
      )
    )
    val locations = List(
      EncounterLocation(
        Some(Address(Some(""), None, Some(""), Some(""), Some(""))),
        Some(
          EncounterLocationAddressType(
            Some(""),
            Some(""),
            Some(""),
            Some("Family Medicine")
          )
        ),
        None
      )
    )
    val expected = Seq(
      List(
        PatientEncounter(
          "337753451",
          insertedAt,
          isStreaming,
          patient1,
          Encounter(
            List(Identifier(Some("4575"), Some("1.2.840.114350.1.13.400.3.7.3.698084.8"))),
            EncounterType(
              "AMB",
              Some("2.16.840.1.113883.5.4"),
              Some("ActCode"),
              Some("Office Visit")
            ),
            Some(dateOrInstant2),
            None,
            providers,
            locations,
            diagnoses = List(),
            reasons = List(),
            draft = false
          )
        ),
        PatientEncounter(
          "337753451",
          insertedAt,
          isStreaming,
          patient1,
          Encounter(
            List(Identifier(Some("4508"), Some("1.2.840.114350.1.13.400.3.7.3.698084.8"))),
            EncounterType(
              "AMB",
              Some("2.16.840.1.113883.5.4"),
              Some("ActCode"),
              Some("Office Visit")
            ),
            Some(dateOrInstant1),
            None,
            providers,
            locations,
            List(),
            List(),
            draft = false
          )
        )
      )
    )

    runWithContext { sc =>
      val p =
        ClinicalSummaryRefresher
          .parsePatientMedicalData(sc.parallelize(data), isStreaming = true)
      p.patientEncounterLists should containInAnyOrder(expected)
    }
  }

  "ClinicalSummaryRefresher.parsePatientMedicalData" should "parse patient vital sign list" in {
    val patient1 = Patient(Some("abc123"), "12345", Datasource("ACPNY"))

    val sourceFile =
      Source.fromFile("src/test/resources/ccd4.json")(Codec("UTF-8"))
    val patientQueryResponse = sourceFile.getLines.mkString
    sourceFile.close

    val data = Seq(
      RedoxMessage(patient1, "PatientAdmin.Discharge", patientQueryResponse, Instant.now())
    )
    val instant = "2017-11-15T15:23:00.000Z"
    val dateOrInstant = {
      DateOrInstant(instant, Some(Instant.parse(instant)), None)
    }
    val expectedVitalSign = VitalSign(
      Value = "120",
      Units = "mm[Hg]",
      DateTime = Some(dateOrInstant),
      Interpretation = "",
      Status = "completed",
      CodeSystemName = "LOINC",
      CodeSystem = "2.16.840.1.113883.6.1",
      Name = "SYSTOLIC BLOOD PRESSURE",
      Code = "8480-6"
    )
    val expected = Seq(
      List(
        PatientVitalSign(
          "337753451",
          patient1,
          expectedVitalSign
        ),
        PatientVitalSign(
          "337753451",
          patient1,
          expectedVitalSign.copy(
            Value = "80",
            Name = "DIASTOLIC BLOOD PRESSURE",
            Code = "8462-4"
          )
        ),
        PatientVitalSign(
          "337753451",
          patient1,
          expectedVitalSign.copy(
            Value = "99",
            Units = "/min",
            Name = "HEART RATE",
            Code = "8867-4"
          )
        ),
        PatientVitalSign(
          "337753451",
          patient1,
          expectedVitalSign
            .copy(
              Value = "37.06",
              Units = "Cel",
              Name = "BODY TEMPERATURE",
              Code = "8310-5"
            )
        ),
        PatientVitalSign(
          "337753451",
          patient1,
          expectedVitalSign
            .copy(
              Value = "17",
              Units = "/min",
              Name = "RESPIRATORY RATE",
              Code = "9279-1"
            )
        ),
        PatientVitalSign(
          "337753451",
          patient1,
          expectedVitalSign
            .copy(
              Value = "152.4",
              Units = "cm",
              Name = "HEIGHT",
              Code = "8302-2",
              CodeSystem = "2.16.840.1.113883.6.1"
            )
        ),
        PatientVitalSign(
          "337753451",
          patient1,
          expectedVitalSign.copy(
            Value = "70.308",
            Units = "kg",
            Name = "WEIGHT",
            Code = "29463-7"
          )
        ),
        PatientVitalSign(
          "337753451",
          patient1,
          expectedVitalSign
            .copy(
              Value = "30.27",
              Units = "kg/m2",
              Name = "BODY MASS INDEX",
              Code = "39156-5"
            )
        ),
        PatientVitalSign(
          "337753451",
          patient1,
          expectedVitalSign
            .copy(
              Value = "99",
              Units = "%",
              Name = "OXYGEN SATURATION",
              Code = "59408-5"
            )
        )
      )
    )

    runWithContext { sc =>
      val p =
        ClinicalSummaryRefresher
          .parsePatientMedicalData(sc.parallelize(data), isStreaming = true)
      p.patientVitalSignLists should containInAnyOrder(expected)
    }
  }

  "ClinicalSummaryRefresher.parsePatientMedicalData" should "parse patient result list" in {
    val patient1 = Patient(Some("abc123"), "12345", Datasource("ACPNY"))

    val sourceFile =
      Source.fromFile("src/test/resources/ccd4.json")(Codec("UTF-8"))
    val patientQueryResponse = sourceFile.getLines.mkString
    sourceFile.close

    val data = Seq(
      RedoxMessage(patient1, "PatientAdmin.Discharge", patientQueryResponse, Instant.now())
    )
    val localDate1 = "2018-03-30"
    val dateOrInstant1 = {
      DateOrInstant(localDate1, None, Some(LocalDate.parse(localDate1)))
    }
    val resultObservations = List(
      ResultObservation(
        Code = Some("NAR"),
        CodeSystem = Some(""),
        CodeSystemName = Some("Epic.ResultText"),
        Name = Some(""),
        Status = Some("completed"),
        Interpretation = Some(""),
        DateTime = Some(dateOrInstant1),
        CodedValue = ResultObservationCodedValue(
          Code = Some(""),
          CodeSystem = Some(""),
          CodeSystemName = Some(""),
          Name = Some("")
        ),
        Value = Some("Ordered by an unspecified provider."),
        ValueType = Some("String"),
        Units = Some(""),
        ReferenceRange = ResultObservationReferenceRange(
          Low = Some(""),
          High = Some(""),
          Text = Some("")
        )
      )
    )
    val expected = Seq(
      List(
        PatientResult(
          "337753451",
          patient1,
          Result(
            Some("GI020"),
            Some("2.16.840.1.113883.6.12"),
            Some("CPT-4"),
            Some("SLEEP STUDY"),
            Some("completed"),
            resultObservations
          )
        ),
        PatientResult(
          "337753451",
          patient1,
          Result(
            Some("ENT061"),
            Some("2.16.840.1.113883.6.12"),
            Some("CPT-4"),
            Some("UROLOGY PHYSICIAN NOTES"),
            Some("completed"),
            resultObservations
          )
        )
      )
    )

    runWithContext { sc =>
      val p =
        ClinicalSummaryRefresher
          .parsePatientMedicalData(sc.parallelize(data), isStreaming = true)
      p.patientResultLists.get should containInAnyOrder(expected)
    }
  }
}
