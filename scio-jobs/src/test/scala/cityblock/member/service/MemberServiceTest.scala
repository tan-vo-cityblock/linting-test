package cityblock.member.service

import cityblock.member.service.TestData.EXPECTED_MEMBER_INSURANCE_RECORD_RESPONSE
import cityblock.member.service.api._
import cityblock.member.service.io.MemberService
import cityblock.member.service.models.PatientInfo.{Datasource, Patient}
import cityblock.utilities.{Backends, Environment}
import com.softwaremill.sttp.testing.SttpBackendStub
import com.softwaremill.sttp.{Id, Response, SttpBackend}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.io.{BufferedSource, Source}

class MemberServiceTest extends FlatSpec with Matchers {
  import MemberServiceTest._

  val simpleEnv: Environment = constructMockEnvironment(DECRYPTED_SECRET, emptySttpBackend)
  val memberServiceEnv: Environment = constructMockEnvironment(DECRYPTED_SECRET, mockSttpBackend)

  "getApiKey" should "extract API_KEY when present" in {
    MemberService.getApiKey(simpleEnv) should be(Right("my_test_api_key"))
  }

  "getApiKey" should "throw an error when there's no API_KEY" in {
    val faultySecret = """runtime: nodejs10
                     |
                     |env_variables:
                     |  NODE_ENV: 'staging'
                     """.stripMargin

    val faultyEnvironment = constructMockEnvironment(faultySecret, emptySttpBackend)
    MemberService.getApiKey(faultyEnvironment) shouldBe a[Left[_, _]]
  }

  "getMemberInsuranceMapping" should "return error for invalid id" in {
    MemberService.getMemberInsuranceMapping(memberId = Some(EXPECTED_MEMBER_ID_1))(simpleEnv) shouldBe a[
      Left[_, _]]
  }

  "getMemberInsuranceMapping" should "return a list of MemberInsuranceRecords pertaining a member" in {
    MemberService.getMemberInsuranceMapping(memberId = Some(EXPECTED_MEMBER_ID_1))(memberServiceEnv) should be(
      Right(EXPECTED_MEMBER_RESULT)
    )
  }

  "getInsurancePatients" should "fetch Patients" in {
    MemberService.getInsurancePatients(carrier = Some("emblem"))(memberServiceEnv) should be(
      Right(
        List(
          Patient(Some(EXPECTED_MEMBER_ID_1), "M1", Datasource("emblem")),
          Patient(Some(EXPECTED_MEMBER_ID_1), "M2", Datasource("emblem")),
          Patient(Some(EXPECTED_MEMBER_ID_2), "M3", Datasource("emblem")),
        )
      )
    )
  }

  "getMrnPatients" should "be able to fetch patients based on their mrn datasource" in {
    val response = MemberService.getMrnPatients(carrier = Option("acpny"))(memberServiceEnv)
    response should be(Right(EXPECTED_PATIENT_MRN))
    response.right.get should have length 2
  }

  "getMemberIdForExternalId" should "return None on empty response" in {
    MemberService.getMemberInsuranceMapping(externalId = Some("M5"), carrier = Some("emblem"))(
      memberServiceEnv) should be(Right(List()))
  }

  "getMemberIdForExternalId" should "return error for invalid id" in {
    MemberService.getMemberInsuranceMapping(
      externalId = Some("Bogus"),
      carrier = Some("datasource"))(memberServiceEnv) shouldBe a[Left[_, _]]
  }

  "getMemberIndexMapping" should "return a list of member insurance records" in {
    MemberService.getMemberInsuranceMapping()(memberServiceEnv) should be(
      Right(EXPECTED_MEMBER_INSURANCE_RECORD_RESPONSE))
  }

  "getMemberIndexMapping" should "return a filtered list of member insurance records" in {
    MemberService.getMemberInsuranceMapping(externalId = Some("M1"), carrier = Some("emblem"))(
      memberServiceEnv) should be(Right(EXPECTED_MEMBER_INSURANCE_RECORD_RESPONSE))
  }

  "publishMemberAttribution" should "return error if member service has an error" in {
    MemberService.publishMemberAttribution(
      EXPECTED_MEMBER_ID_1,
      TestData.testMemberCreateAndPublishRequest
    )(simpleEnv) shouldBe a[Left[_, _]]
  }

  "publishMemberAttribution" should "return the messageId sent from the member service" in {
    MemberService.publishMemberAttribution(
      EXPECTED_MEMBER_ID_1,
      TestData.testMemberCreateAndPublishRequest
    )(memberServiceEnv) should be(
      Right(
        MemberCreateAndPublishResponse(EXPECTED_MEMBER_ID_1, "10000004", None, "testMessageId")))
  }
}

object MemberServiceTest extends MockFactory {
  val EXPECTED_MEMBER_ID_1 = "a3ccab32-d960-11e9-b350-acde48001122"
  val EXPECTED_MEMBER_ID_2 = "a3cdcb8e-d960-11e9-b350-acde48001122"

  val EXPECTED_MEMBER_RESULT: List[MemberInsuranceRecord] =
    List(
      MemberInsuranceRecord(memberId = EXPECTED_MEMBER_ID_1,
                            externalId = "M1",
                            carrier = "emblem",
                            current = Some(true)),
      MemberInsuranceRecord(memberId = EXPECTED_MEMBER_ID_1,
                            externalId = "M2",
                            carrier = "emblem",
                            current = Some(true)),
    )

  val EXPECTED_PATIENT_MRN = List(
    Patient(Some(EXPECTED_MEMBER_ID_2), "epicId", Datasource(name = "acpny")),
    Patient(Some(EXPECTED_MEMBER_ID_2), "anotherEpicId", Datasource(name = "acpny")))

  // List of all responses
  val EMPTY_MEMBER_RESPONSE: String = getJsonFromFile("EmptyMemberResponse.json")
  val ALL_MEMBER_INSURANCE_RESPONSE: String = getJsonFromFile("SampleAllMembersResponse.json")
  val ALL_MEMBER_MRN_RESPONSE: String = getJsonFromFile("MemberDatasourceFilterResponse.json")
  val SINGLE_MEMBER_RESPONSE: String = getJsonFromFile("SampleMemberResponse.json")
  val MEMBER_INSURANCE_RECORDS_RESPONSE: String = getJsonFromFile(
    fileName = "MemberInsuranceRecordsResponse.json")
  val DECRYPTED_SECRET: String = """runtime: nodejs10
    |
    |env_variables:
    |  NODE_ENV: 'staging'
    |  API_KEY: 'my_test_api_key'""".stripMargin

  @SerialVersionUID(1L)
  class TestBackends(fakeSttpBackend: SttpBackend[Id, Nothing]) extends Backends {
    @transient override lazy val sttpBackend: SttpBackend[Id, Nothing] = fakeSttpBackend
  }
  // Used to mock our environment to allow us to mock the responses and stub the secrets to make the requests
  def constructMockEnvironment(
    memberServiceSecretData: String,
    sttpBackend: SttpBackend[Id, Nothing]
  ): Environment =
    Environment.Test.copy(
      memberServiceSecretData = memberServiceSecretData,
      backends = new TestBackends(sttpBackend)
    )

  // Used in order to get our test server responses from the Member Service
  private def getJsonFromFile(fileName: String): String = {
    val filePath = s"src/test/resources/memberService/$fileName"
    val file: BufferedSource = Source.fromFile(filePath)
    val str = file.getLines.mkString
    file.close
    str
  }

  val emptySttpBackend: SttpBackendStub[Id, Nothing] = SttpBackendStub.synchronous

  val mockSttpBackend: SttpBackendStub[Id, Nothing] = SttpBackendStub.synchronous
    .whenRequestMatchesPartial({
      case r if r.uri.toString().endsWith(EXPECTED_MEMBER_ID_1) =>
        Response.ok(SINGLE_MEMBER_RESPONSE)
      case r if r.uri.toString().endsWith("members/insurance/records?carrier=emblem") =>
        Response.ok(ALL_MEMBER_INSURANCE_RESPONSE)
      case r
          if r.uri.toString().endsWith("members/insurance/records?externalId=M5&carrier=emblem") =>
        Response.ok(EMPTY_MEMBER_RESPONSE)
      case r if r.uri.toString().endsWith("members/mrn/records?carrier=acpny") =>
        Response.ok(ALL_MEMBER_MRN_RESPONSE)
      case r if r.uri.toString().endsWith(s"members/${EXPECTED_MEMBER_ID_1}/updateAndPublish") =>
        Response.ok(
          "{ \"messageId\": \"testMessageId\", \"patientId\": \"a3ccab32-d960-11e9-b350-acde48001122\", \"cityblockId\": \"10000004\", \"mrn\": null }")
      case r if r.uri.toString().endsWith("members/insurance/records") =>
        Response.ok(MEMBER_INSURANCE_RECORDS_RESPONSE)
      case r
          if r.uri.toString().endsWith("members/insurance/records?externalId=M1&carrier=emblem") =>
        Response.ok(MEMBER_INSURANCE_RECORDS_RESPONSE)
    })
}
