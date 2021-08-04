package cityblock.transforms.redox

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cityblock.ehrmodels.redox.service.{RedoxApiClient, RedoxApiQueryResponse}
import cityblock.member.service.models.PatientInfo.{Datasource, Patient}
import cityblock.streaming.jobs.aggregations.Encounters._
import cityblock.transforms.redox.EnrichEncounter.prepareVisitQueryString
import cityblock.utilities.Environment
import cityblock.utilities.time.DateOrInstant
import com.github.vitalsoftware.scalaredox.client.RedoxResponse
import com.github.vitalsoftware.scalaredox.models.VisitQuery
import play.api.libs.json.{JsError, JsValue, Json, Reads, Writes}
import org.joda.time.{DateTimeZone, Instant, LocalDate}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.io.{Codec, Source}

trait MockRedoxClient {
  def get[T, U](query: T)(implicit writes: Writes[T], reads: Reads[U]): Future[RedoxResponse[U]]
}

object EnrichEncounterTest extends MockRedoxClient with RedoxApiClient {
  implicit override def system: ActorSystem = ActorSystem("TEST")
  implicit override def materializer: ActorMaterializer = ActorMaterializer()(system)

  override def get[VisitQuery, JsValue](
    query: VisitQuery
  )(implicit writes: Writes[VisitQuery], reads: Reads[JsValue]): Future[RedoxResponse[JsValue]] = {
    val visitSourceFile = Source.fromFile("src/test/resources/encounterVisit1.json")(Codec("UTF-8"))
    val jsonVisit = Json
      .fromJson[JsValue](Json.parse(visitSourceFile.getLines.mkString))
      .fold(
        invalid = err => throw new RuntimeException(JsError.toJson(JsError(err)).toString()),
        valid = identity
      )
    visitSourceFile.close
    Future(RedoxResponse(Right(jsonVisit)))
  }
}

class EnrichEncounterTest extends FlatSpec with Matchers with MockFactory {
  implicit val environment: Environment = Environment.Test
  private val insertedAt = Instant.ofEpochMilli(0)
  private val isStreaming = true
  val patient1: Patient = Patient(Some("abc123"), "12345", Datasource("ACPNY"))
  val localDate2 = "2018-04-19"
  val localDate1 = "2018-04-12"
  val dateOrInstant2: DateOrInstant =
    DateOrInstant(localDate2, None, Some(LocalDate.parse(localDate2)))
  val dateOrInstant1: DateOrInstant =
    DateOrInstant(localDate1, None, Some(LocalDate.parse(localDate1)))
  val testPatientEncounter: PatientEncounter = PatientEncounter(
    "337753451",
    Some(insertedAt),
    isStreaming,
    patient1,
    Encounter(
      List(Identifier(Some("230555555"), Some("1.2.840.114350.1.13.400.3.7.3.698084.8"))),
      EncounterType("AMB", Some("2.16.840.1.113883.5.4"), Some("ActCode"), Some("Office Visit")),
      dateTime = Some(dateOrInstant2),
      endDateTime = None,
      providers = List(
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
      ),
      locations = List(
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
      ),
      diagnoses = List(),
      reasons = List(),
      draft = false
    )
  )
  val mockRedoxClient: MockRedoxClient = stub[MockRedoxClient]

  "EnrichEncounter.addEncounterDetails" should "create a valid VisitQuery with proper date time" in {
    val visit = prepareVisitQueryString(
      patient1.externalId,
      testPatientEncounter.encounter.dateTime,
      testPatientEncounter.encounter.endDateTime
    )
    val visitQuery = RedoxApiQueryResponse.createQuery[VisitQuery](visit)
    visitQuery.Visit.StartDateTime should equal(
      dateOrInstant2.date.get.toDateTimeAtStartOfDay(DateTimeZone.UTC)
    )
    visitQuery.Visit.EndDateTime shouldBe None
  }

  "EnrichEncounter.addEncounterDetails" should "enrich patient encounter with valid note" in {
    val visit = prepareVisitQueryString(
      patient1.externalId,
      testPatientEncounter.encounter.dateTime,
      testPatientEncounter.encounter.endDateTime
    )
    val visitQuery = RedoxApiQueryResponse.createQuery[VisitQuery](visit)
    (mockRedoxClient
      .get[VisitQuery, JsValue](_: VisitQuery)(_: Writes[VisitQuery], _: Reads[JsValue]))
      .when(visitQuery, *, *)
      .returns(
        EnrichEncounterTest
          .get[VisitQuery, JsValue](visitQuery)
      )
    val visitResp = mockRedoxClient.get[VisitQuery, JsValue](visitQuery)
    val encounterWithNote =
      EnrichEncounter.addDetailsToEncounterReason(testPatientEncounter.encounter, visitResp)(
        patient1.externalId
      )
    encounterWithNote.reasons.head.notes.get should (include("Electronically signed by Sanza Stark") and include(
      "John Snow is a 25 year old male"
    ))
    encounterWithNote.reasons.head.name.get should include("Medication Refill")
    encounterWithNote.reasons.head.notes.get shouldNot (include("""<br>""") and include(
      """<\br>"""
    ))
  }

  "EnrichEncounter.addEncounterDetails" should "return nothing if visitIds do not match" in {
    val wrongIdEncounter =
      testPatientEncounter.copy(
        encounter =
          testPatientEncounter.encounter.copy(identifiers = List(Identifier(Some(""), Some(""))))
      )
    val visit = prepareVisitQueryString(
      patient1.externalId,
      wrongIdEncounter.encounter.dateTime,
      wrongIdEncounter.encounter.endDateTime
    )
    val visitQuery = RedoxApiQueryResponse.createQuery[VisitQuery](visit)
    (mockRedoxClient
      .get[VisitQuery, JsValue](_: VisitQuery)(_: Writes[VisitQuery], _: Reads[JsValue]))
      .when(visitQuery, *, *)
      .returns(
        EnrichEncounterTest
          .get[VisitQuery, JsValue](visitQuery)
      )
    val visitResp = mockRedoxClient.get[VisitQuery, JsValue](visitQuery)
    val encounterWithNote =
      EnrichEncounter.addDetailsToEncounterReason(wrongIdEncounter.encounter, visitResp)(
        patient1.externalId
      )
    encounterWithNote.reasons shouldBe List()
  }
}
