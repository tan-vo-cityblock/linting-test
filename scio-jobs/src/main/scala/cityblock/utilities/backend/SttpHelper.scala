package cityblock.utilities.backend

import cityblock.utilities.Loggable
import com.softwaremill.sttp._

import scala.io.{BufferedSource, Source}
import com.softwaremill.sttp.testing.SttpBackendStub

/**
 * Object facilitates backend integration testing of scio jobs by providing a mock sttp backend.
 * Otherwise, a production sttp backend val is provided.
 */
object SttpHelper extends Loggable {
  def getBackend(testing: Boolean = false): SttpBackend[Id, Nothing] =
    if (testing) {
      mockBackend
    } else {
      prodBackend
    }

  private lazy val prodBackend: SttpBackend[Id, Nothing] =
    HttpURLConnectionBackend()

  private val mockPatientId = "151523299033089"
  private val mockPhysicianId = "150679616754852"

  private val sandBoxEndPt = "https://sandbox.elationemr.com/api/2.0"
  private val mockResponsePath = "src/test/resources/elationEvents/"

  private val mockAuthUri = uri"$sandBoxEndPt/oauth2/token/"
  private val mockProblem1Uri =
    uri"$sandBoxEndPt/problems/?limit=50&patient=$mockPatientId"
  private val mockProblem2Uri =
    uri"$sandBoxEndPt/problems/?limit=1&offset=1&patient=$mockPatientId"
  private val mockProblem3Uri =
    uri"$sandBoxEndPt/problems/?limit=1&offset=2&patient=$mockPatientId"
  private val mockVitalsUri =
    uri"$sandBoxEndPt/vitals/?limit=50&patient=$mockPatientId"
  private val mockCcdaUri = uri"$sandBoxEndPt/ccda/$mockPatientId/"
  private val mockAllPracticesUri = uri"$sandBoxEndPt/practices/?limit=50"
  private val mockPhysicianUri = uri"$sandBoxEndPt/physicians/$mockPhysicianId/"
  private val mockLabHookUri = uri"$sandBoxEndPt/reports/91143209000"

  private lazy val uriPostResponseMpa = Map(
    mockAuthUri -> convertFileToString(s"${mockResponsePath}mockAuthResponse.json")
  )
  private lazy val uriGetResponseMap = Map(
    mockProblem1Uri -> convertFileToString(s"${mockResponsePath}mockProblemPullResponse1.json"),
    mockProblem2Uri -> convertFileToString(s"${mockResponsePath}mockProblemPullResponse2.json"),
    mockProblem3Uri -> convertFileToString(s"${mockResponsePath}mockProblemPullResponse3.json"),
    mockVitalsUri -> convertFileToString(s"${mockResponsePath}mockVitalPullResponse.json"),
    mockCcdaUri -> convertFileToString(s"${mockResponsePath}mockCcdaPullResponse.json"),
    mockAllPracticesUri -> convertFileToString(
      s"${mockResponsePath}mockAllPracticesPullResponse.json"
    ),
    mockPhysicianUri -> convertFileToString(
      s"${mockResponsePath}mockPhysicianPullResponse.json"
    ),
    mockLabHookUri -> convertFileToString(s"${mockResponsePath}mockLabPullResponse.json")
  )

  private lazy val mockBackend: SttpBackendStub[Id, Nothing] =
    SttpBackendStub.synchronous
      .whenRequestMatchesPartial({
        case r
            if uriPostResponseMpa
              .contains(r.uri) && r.method == Method.POST =>
          Response.ok(uriPostResponseMpa(r.uri))

        case r if uriGetResponseMap.contains(r.uri) && r.method == Method.GET =>
          Response.ok(uriGetResponseMap(r.uri))

        case r =>
          logger.info(s"sttp backend stub request did not match conditions: uri = ${r.uri}")
          Response.error("Bad URI and/or Method", 400, "Bad URI and/or Method")
      })

  private def convertFileToString(path: String): String = {
    val file: BufferedSource = Source.fromFile(path)
    val str = file.getLines.mkString
    file.close
    str
  }
}
