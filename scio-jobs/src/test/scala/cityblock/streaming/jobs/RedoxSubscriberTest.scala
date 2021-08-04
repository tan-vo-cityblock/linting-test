package cityblock.streaming.jobs

import cityblock.member.service.models.PatientInfo.{Datasource, Patient}
import cityblock.streaming.jobs.RedoxSubscriber.RedoxMessage
import cityblock.streaming.jobs.orders.Orders._
import cityblock.streaming.jobs.scheduling.Diagnosis.ParsedDiagnosis
import cityblock.streaming.jobs.scheduling.Event.ParsedEvent
import cityblock.streaming.jobs.scheduling.Provider.ParsedProvider
import cityblock.utilities.time.DateOrInstant
import com.spotify.scio.testing._
import org.joda.time.Instant

import scala.io.Source

class RedoxSubscriberTest extends PipelineSpec {
  "RedoxSubscriber.parsePatientOrders" should "parse patient orders from ORU messages" in {
    val patient = Patient(Some("abc123"), "12345", Datasource("ACPNY"))

    val sourceFile = Source.fromFile("src/test/resources/oru.json")
    val patientORUMessage = sourceFile.getLines.mkString
    sourceFile.close

    val data = Seq(
      RedoxMessage(patient, "Results.New", patientORUMessage, Instant.now())
    )
    val timestamp = "2018-01-01T09:10:00.000Z"
    val dateOrInstant =
      DateOrInstant(timestamp, Some(Instant.parse(timestamp)), None)
    val expected = Seq(
      List(
        PatientOrder(
          messageId = "12345678",
          patient = patient,
          order = Order(
            ID = "1234567",
            ApplicationOrderID = Some("012345567"),
            TransactionDateTime = None,
            CollectionDateTime = Some(dateOrInstant),
            CompletionDateTime = Some(dateOrInstant),
            Notes = List(),
            ResultsStatus = Some("Final"),
            Procedure = Some(
              Procedure(
                Code = Some("82565"),
                Codeset = None,
                Description = Some("CREATININE, SERUM")
              )
            ),
            Provider = Some(
              Provider(
                NPI = Some("01234567"),
                ID = None,
                IDType = None,
                FirstName = Some("THOMAS"),
                LastName = Some("SMITH"),
                Credentials = List(),
                Address = Some(
                  Address(
                    StreetAddress = None,
                    City = None,
                    State = None,
                    ZIP = None,
                    County = None,
                    Country = None
                  )
                ),
                Location = Some(
                  Location(
                    Type = None,
                    Facility = None,
                    Department = None,
                    Room = None
                  )
                ),
                PhoneNumber = Some(
                  PhoneNumber(
                    Office = None
                  )
                )
              )
            ),
            ResultCopyProviders = List(),
            Status = "Resulted",
            ResponseFlag = None,
            Priority = Some("Routine"),
            Results = List(
              Result(
                Code = Some("1111-0"),
                Codeset = None,
                Description = Some("Creat SerPl-mCnc"),
                Specimen = Some(
                  ResultSpecimen(
                    Source = None,
                    BodySite = None,
                    ID = None
                  )
                ),
                Value = "1.45",
                ValueType = "Numeric",
                CompletionDateTime = None,
                FileType = None,
                Units = Some("mg/dL"),
                Notes = List(
                  " The upper reference limit for Creatinine is approximately",
                  " 13% higher for people identified as"
                ),
                AbnormalFlag = Some("High"),
                Status = "Final",
                Producer = Some(
                  ResultProducer(
                    ID = Some("QUEST DIAGNOSTICS"),
                    Name = None,
                    IDType = None,
                    Address = Some(
                      Address(
                        StreetAddress = None,
                        City = None,
                        State = None,
                        ZIP = None,
                        County = None,
                        Country = None
                      )
                    )
                  )
                ),
                Performer = Some(
                  ResultPerformer(
                    ID = None,
                    IDType = None,
                    FirstName = None,
                    LastName = None,
                    Credentials = List(),
                    Address = Some(
                      Address(
                        StreetAddress = None,
                        City = None,
                        State = None,
                        ZIP = None,
                        County = None,
                        Country = None
                      )
                    ),
                    Location = Some(
                      Location(
                        Type = None,
                        Facility = None,
                        Department = None,
                        Room = None
                      )
                    ),
                    PhoneNumber = None
                  )
                ),
                ReferenceRange = Some(
                  ResultReferenceRange(
                    Low = None,
                    High = None,
                    Text = Some("0.60-0.93")
                  )
                ),
                ObservationMethod = Some(
                  ResultObservationMethod(
                    Code = None,
                    Codeset = None,
                    Description = None
                  )
                )
              ),
              Result(
                Code = Some("2222-3"),
                Codeset = None,
                Description = Some("GFR/BSA.pred SerPl MDRD-ArVRat"),
                Specimen = Some(
                  ResultSpecimen(
                    Source = None,
                    BodySite = None,
                    ID = None
                  )
                ),
                Value = "35",
                ValueType = "Numeric",
                CompletionDateTime = None,
                FileType = None,
                Units = Some("mL/min/1.73m2"),
                Notes = List(),
                AbnormalFlag = Some("Low"),
                Status = "Final",
                Producer = Some(
                  ResultProducer(
                    ID = Some("QUEST DIAGNOSTICS"),
                    Name = None,
                    IDType = None,
                    Address = Some(
                      Address(
                        StreetAddress = None,
                        City = None,
                        State = None,
                        ZIP = None,
                        County = None,
                        Country = None
                      )
                    )
                  )
                ),
                Performer = Some(
                  ResultPerformer(
                    ID = None,
                    IDType = None,
                    FirstName = None,
                    LastName = None,
                    Credentials = List(),
                    Address = Some(
                      Address(
                        StreetAddress = None,
                        City = None,
                        State = None,
                        ZIP = None,
                        County = None,
                        Country = None
                      )
                    ),
                    Location = Some(
                      Location(
                        Type = None,
                        Facility = None,
                        Department = None,
                        Room = None
                      )
                    ),
                    PhoneNumber = None
                  )
                ),
                ReferenceRange = Some(
                  ResultReferenceRange(
                    Low = None,
                    High = None,
                    Text = Some(">=60")
                  )
                ),
                ObservationMethod = Some(
                  ResultObservationMethod(
                    Code = None,
                    Codeset = None,
                    Description = None
                  )
                )
              )
            )
          )
        )
      )
    )

    runWithContext { sc =>
      val p = RedoxSubscriber.filterAndParsePatientOrders(sc.parallelize(data))
      p should containInAnyOrder(expected)
    }
  }

  "RedoxSubscriber.decodeSchedulingEvent" should "decode an SIU message with an all-null appointment info" in {
    val sourceFile = Source.fromFile("src/test/resources/siu1.json")
    val patientSchedulingMessage = sourceFile.getLines.mkString
    sourceFile.close

    val expected = ParsedEvent(
      eventType = "New",
      transmissionId = 622679638,
      visitId = "207368879",
      dateTime = "2019-01-05T01:00:00.000Z",
      duration = 30,
      status = Some("Scheduled"),
      reason = Some("INITIAL CONSULT"),
      cancelReason = None,
      instructions = Some(List("Instruction 1", "Instruction 2", "Instruction 3")),
      facility = Some("BXGC"),
      facilityType = None,
      facilityDepartment = Some("GCBD PRIMARY CARE"),
      facilityRoom = None,
      provider = ParsedProvider(
        id = None,
        idType = None,
        credentials = Some(List()),
        firstName = Some("Md"),
        lastName = Some("Fammd")
      ),
      attendingProvider = ParsedProvider(
        id = None,
        idType = None,
        credentials = Some(List()),
        firstName = None,
        lastName = None
      ),
      consultingProvider = Some(
        ParsedProvider(
          id = None,
          idType = None,
          credentials = Some(List()),
          firstName = None,
          lastName = None
        )
      ),
      referringProvider = Some(
        ParsedProvider(
          id = None,
          idType = None,
          credentials = Some(List()),
          firstName = None,
          lastName = None
        )
      ),
      diagnoses = List(
        ParsedDiagnosis(
          code = None,
          codeset = None,
          name = None,
          diagnosisType = None
        )
      ),
      providerSchedulingNotes = List()
    )

    runWithContext { sc =>
      val p = RedoxSubscriber.decodeSchedulingEvent(patientSchedulingMessage)
      p should equal(Right(expected))
    }
  }

  "RedoxSubscriber.decodeSchedulingEvent" should "decode an SIU message with an appointment info with description" in {
    val sourceFile = Source.fromFile("src/test/resources/siu2.json")
    val patientSchedulingMessage = sourceFile.getLines.mkString
    sourceFile.close

    val expected = ParsedEvent(
      eventType = "New",
      transmissionId = 622679638,
      visitId = "207368879",
      dateTime = "2019-01-05T01:00:00.000Z",
      duration = 30,
      status = Some("Scheduled"),
      reason = Some("INITIAL CONSULT"),
      cancelReason = None,
      instructions = Some(List("Instruction 1", "Instruction 2", "Instruction 3")),
      facility = Some("BXGC"),
      facilityType = None,
      facilityDepartment = Some("GCBD PRIMARY CARE"),
      facilityRoom = None,
      provider = ParsedProvider(
        id = None,
        idType = None,
        credentials = Some(List()),
        firstName = Some("Md"),
        lastName = Some("Fammd")
      ),
      attendingProvider = ParsedProvider(
        id = None,
        idType = None,
        credentials = Some(List()),
        firstName = None,
        lastName = None
      ),
      consultingProvider = Some(
        ParsedProvider(
          id = None,
          idType = None,
          credentials = Some(List()),
          firstName = None,
          lastName = None
        )
      ),
      referringProvider = Some(
        ParsedProvider(
          id = None,
          idType = None,
          credentials = Some(List()),
          firstName = None,
          lastName = None
        )
      ),
      diagnoses = List(
        ParsedDiagnosis(
          code = None,
          codeset = None,
          name = None,
          diagnosisType = None
        )
      ),
      providerSchedulingNotes = List("A scheduling note")
    )

    runWithContext { sc =>
      val p = RedoxSubscriber.decodeSchedulingEvent(patientSchedulingMessage)
      p should equal(Right(expected))
    }
  }

  "RedoxSubscriber.decodeSchedulingEvent" should "decode an SIU message with an empty appointment info array" in {
    val sourceFile = Source.fromFile("src/test/resources/siu3.json")
    val patientSchedulingMessage = sourceFile.getLines.mkString
    sourceFile.close

    val expected = ParsedEvent(
      eventType = "New",
      transmissionId = 622679638,
      visitId = "207368879",
      dateTime = "2019-01-05T01:00:00.000Z",
      duration = 30,
      status = Some("Scheduled"),
      reason = Some("INITIAL CONSULT"),
      cancelReason = None,
      instructions = Some(List("Instruction 1", "Instruction 2", "Instruction 3")),
      facility = Some("BXGC"),
      facilityType = None,
      facilityDepartment = Some("GCBD PRIMARY CARE"),
      facilityRoom = None,
      provider = ParsedProvider(
        id = None,
        idType = None,
        credentials = Some(List()),
        firstName = Some("Md"),
        lastName = Some("Fammd")
      ),
      attendingProvider = ParsedProvider(
        id = None,
        idType = None,
        credentials = Some(List()),
        firstName = None,
        lastName = None
      ),
      consultingProvider = Some(
        ParsedProvider(
          id = None,
          idType = None,
          credentials = Some(List()),
          firstName = None,
          lastName = None
        )
      ),
      referringProvider = Some(
        ParsedProvider(
          id = None,
          idType = None,
          credentials = Some(List()),
          firstName = None,
          lastName = None
        )
      ),
      diagnoses = List(
        ParsedDiagnosis(
          code = None,
          codeset = None,
          name = None,
          diagnosisType = None
        )
      ),
      providerSchedulingNotes = List()
    )

    runWithContext { sc =>
      val p = RedoxSubscriber.decodeSchedulingEvent(patientSchedulingMessage)
      p should equal(Right(expected))
    }
  }

  "RedoxSubscriber.decodeSchedulingEvent" should "decode an SIU message with an two null appointment infos" in {
    val sourceFile = Source.fromFile("src/test/resources/siu4.json")
    val patientSchedulingMessage = sourceFile.getLines.mkString
    sourceFile.close

    val expected = ParsedEvent(
      eventType = "New",
      transmissionId = 622679638,
      visitId = "207368879",
      dateTime = "2019-01-05T01:00:00.000Z",
      duration = 30,
      status = Some("Scheduled"),
      reason = Some("INITIAL CONSULT"),
      cancelReason = None,
      instructions = Some(List("Instruction 1", "Instruction 2", "Instruction 3")),
      facility = Some("BXGC"),
      facilityType = None,
      facilityDepartment = Some("GCBD PRIMARY CARE"),
      facilityRoom = None,
      provider = ParsedProvider(
        id = None,
        idType = None,
        credentials = Some(List()),
        firstName = Some("Md"),
        lastName = Some("Fammd")
      ),
      attendingProvider = ParsedProvider(
        id = None,
        idType = None,
        credentials = Some(List()),
        firstName = None,
        lastName = None
      ),
      consultingProvider = Some(
        ParsedProvider(
          id = None,
          idType = None,
          credentials = Some(List()),
          firstName = None,
          lastName = None
        )
      ),
      referringProvider = Some(
        ParsedProvider(
          id = None,
          idType = None,
          credentials = Some(List()),
          firstName = None,
          lastName = None
        )
      ),
      diagnoses = List(
        ParsedDiagnosis(
          code = None,
          codeset = None,
          name = None,
          diagnosisType = None
        )
      ),
      providerSchedulingNotes = List()
    )

    runWithContext { sc =>
      val p = RedoxSubscriber.decodeSchedulingEvent(patientSchedulingMessage)
      p should equal(Right(expected))
    }
  }

  "RedoxSubscriber.decodeSchedulingEvent" should "decode an SIU message with one null and one non-null appointment infos" in {
    val sourceFile = Source.fromFile("src/test/resources/siu5.json")
    val patientSchedulingMessage = sourceFile.getLines.mkString
    sourceFile.close

    val expected = ParsedEvent(
      eventType = "New",
      transmissionId = 622679638,
      visitId = "207368879",
      dateTime = "2019-01-05T01:00:00.000Z",
      duration = 30,
      status = Some("Scheduled"),
      reason = Some("INITIAL CONSULT"),
      cancelReason = None,
      instructions = Some(List("Instruction 1", "Instruction 2", "Instruction 3")),
      facility = Some("BXGC"),
      facilityType = None,
      facilityDepartment = Some("GCBD PRIMARY CARE"),
      facilityRoom = None,
      provider = ParsedProvider(
        id = None,
        idType = None,
        credentials = Some(List()),
        firstName = Some("Md"),
        lastName = Some("Fammd")
      ),
      attendingProvider = ParsedProvider(
        id = None,
        idType = None,
        credentials = Some(List()),
        firstName = None,
        lastName = None
      ),
      consultingProvider = Some(
        ParsedProvider(
          id = None,
          idType = None,
          credentials = Some(List()),
          firstName = None,
          lastName = None
        )
      ),
      referringProvider = Some(
        ParsedProvider(
          id = None,
          idType = None,
          credentials = Some(List()),
          firstName = None,
          lastName = None
        )
      ),
      diagnoses = List(
        ParsedDiagnosis(
          code = None,
          codeset = None,
          name = None,
          diagnosisType = None
        )
      ),
      providerSchedulingNotes = List("A scheduling note")
    )

    runWithContext { sc =>
      val p = RedoxSubscriber.decodeSchedulingEvent(patientSchedulingMessage)
      p should equal(Right(expected))
    }
  }

  "RedoxSubscriber.decodeSchedulingEvent" should "decode an SIU message with two non-null appointment infos" in {
    val sourceFile = Source.fromFile("src/test/resources/siu6.json")
    val patientSchedulingMessage = sourceFile.getLines.mkString
    sourceFile.close

    val expected = ParsedEvent(
      eventType = "New",
      transmissionId = 622679638,
      visitId = "207368879",
      dateTime = "2019-01-05T01:00:00.000Z",
      duration = 30,
      status = Some("Scheduled"),
      reason = Some("INITIAL CONSULT"),
      cancelReason = None,
      instructions = Some(List("Instruction 1", "Instruction 2", "Instruction 3")),
      facility = Some("BXGC"),
      facilityType = None,
      facilityDepartment = Some("GCBD PRIMARY CARE"),
      facilityRoom = None,
      provider = ParsedProvider(
        id = None,
        idType = None,
        credentials = Some(List()),
        firstName = Some("Md"),
        lastName = Some("Fammd")
      ),
      attendingProvider = ParsedProvider(
        id = None,
        idType = None,
        credentials = Some(List()),
        firstName = None,
        lastName = None
      ),
      consultingProvider = Some(
        ParsedProvider(
          id = None,
          idType = None,
          credentials = Some(List()),
          firstName = None,
          lastName = None
        )
      ),
      referringProvider = Some(
        ParsedProvider(
          id = None,
          idType = None,
          credentials = Some(List()),
          firstName = None,
          lastName = None
        )
      ),
      diagnoses = List(
        ParsedDiagnosis(
          code = None,
          codeset = None,
          name = None,
          diagnosisType = None
        )
      ),
      providerSchedulingNotes = List("A first scheduling note", "A second scheduling note")
    )

    runWithContext { sc =>
      val p = RedoxSubscriber.decodeSchedulingEvent(patientSchedulingMessage)
      p should equal(Right(expected))
    }
  }
}
