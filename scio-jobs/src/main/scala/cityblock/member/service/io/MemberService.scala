package cityblock.member.service.io

import cityblock.member.service.models.PatientInfo._
import cityblock.member.service.api._
import cityblock.utilities.Environment
import cityblock.utilities.backend.{ApiError, TokenError, UnknownResponseError}
import com.softwaremill.sttp._
import com.softwaremill.sttp.circe._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._
import net.jcazevedo.moultingyaml._

object MemberService {
  private val API_VERSION = "1.0"
  def memberEndpoint(implicit environment: Environment): String =
    s"${environment.memberServiceUrl}/$API_VERSION"

  def getYamlAsString(implicit environment: Environment): String =
    environment.memberServiceSecretData

  def getApiKey(implicit environment: Environment): Either[ApiError, String] = {
    val yamlAsString: String = getYamlAsString(environment)
    val yaml = yamlAsString.parseYaml.asYamlObject()

    yaml.getFields(YamlString("env_variables")) match {
      case Seq(varsYamlObject) =>
        val varsAsMap = varsYamlObject.convertTo[Map[String, String]]
        varsAsMap.get("API_KEY") match {
          case Some(apiKey) => Right(apiKey)
          case _            => Left(TokenError("Failed to extract API key"))
        }
      case _ => Left(TokenError("Failed to extract API key"))
    }
  }

  def createMember(
    createRequest: MemberCreateRequest
  )(implicit environment: Environment): Either[ApiError, MemberCreateResponse] = {
    implicit val backend = environment.backends.sttpBackend

    val url = uri"$memberEndpoint/members"

    // TODO: Don't refetch the API key for each create request
    getApiKey match {
      case Right(apiKey) =>
        val request = sttp
          .post(url)
          .header("apiKey", apiKey)
          .body(createRequest)
          .response(asJson[MemberCreateResponse])
        val ret = request.send.body

        ApiError
          .convertResponse[MemberCreateResponse, UnknownResponseError](ret, Some(url.toString))
      case Left(e) => Left(e)
    }
  }

  def publishMemberAttribution(
    memberId: String,
    publishRequest: MemberCreateAndPublishRequest
  )(implicit environment: Environment): Either[ApiError, MemberCreateAndPublishResponse] = {
    implicit val backend = environment.backends.sttpBackend

    val url = uri"$memberEndpoint/members/$memberId/updateAndPublish"

    getApiKey match {
      case Right(apiKey) =>
        val request = sttp
          .post(url)
          .header("apiKey", apiKey)
          .body(publishRequest)
          .response(asJson[MemberCreateAndPublishResponse])

        val ret = request.send.body
        ApiError
          .convertResponse[MemberCreateAndPublishResponse, UnknownResponseError](
            ret,
            Some(url.toString)
          )

      case Left(e) => Left(e)
    }
  }

  def createAndPublishMember(
    createAndPublishRequest: MemberCreateAndPublishRequest
  )(implicit environment: Environment): Either[ApiError, MemberCreateAndPublishResponse] = {
    implicit val backend = environment.backends.sttpBackend

    val url = uri"$memberEndpoint/members/createAndPublish"

    getApiKey match {
      case Right(apiKey) =>
        val request = sttp
          .post(url)
          .header("apiKey", apiKey)
          .body(createAndPublishRequest)
          .response(asJson[MemberCreateAndPublishResponse])

        val ret = request.send.body
        ApiError
          .convertResponse[MemberCreateAndPublishResponse, UnknownResponseError](
            ret,
            Some(url.toString)
          )

      case Left(e) => Left(e)
    }
  }

  def getMemberMrn(
    memberId: String
  )(implicit environment: Environment): Either[ApiError, List[MemberInsuranceRecord]] =
    getMemberMrnMapping(memberId = Option(memberId))

  def getMrnPatients(
    memberId: Option[String] = None,
    externalId: Option[String] = None,
    carrier: Option[String] = None
  )(implicit environment: Environment): Either[ApiError, List[Patient]] =
    getMemberMrnMapping(memberId = memberId, externalId = externalId, carrier = carrier)
      .map(mrnRecords => mrnRecords.map(_.toPatient))

  def getInsurancePatients(
    memberId: Option[String] = None,
    externalId: Option[String] = None,
    carrier: Option[String] = None
  )(implicit environment: Environment): Either[ApiError, List[Patient]] =
    getMemberInsuranceMapping(memberId = memberId, externalId = externalId, carrier = carrier)
      .map(insuranceRecords => insuranceRecords.map(_.toPatient))

  def getMemberInsuranceMapping(
    memberId: Option[String] = None,
    externalId: Option[String] = None,
    carrier: Option[String] = None
  )(implicit environment: Environment): Either[ApiError, List[MemberInsuranceRecord]] = {
    implicit val backend = environment.backends.sttpBackend

    val url =
      uri"${memberEndpoint}/members/insurance/records?memberId=${memberId}&externalId=${externalId}&carrier=${carrier}"

    getApiKey match {
      case Right(apiKey) =>
        val request = sttp
          .get(url)
          .header("apiKey", apiKey)
          .response(asJson[List[MemberInsuranceRecord]])

        val ret = request.send.body

        ApiError
          .convertResponse[List[MemberInsuranceRecord], UnknownResponseError](
            ret,
            Some(url.toString)
          )

      case Left(e) => Left(e)
    }
  }

  def getMemberMrnMapping(
    memberId: Option[String] = None,
    externalId: Option[String] = None,
    carrier: Option[String] = None
  )(implicit environment: Environment): Either[ApiError, List[MemberInsuranceRecord]] = {
    implicit val backend = environment.backends.sttpBackend

    val url =
      uri"${memberEndpoint}/members/mrn/records?memberId=${memberId}&externalId=${externalId}&carrier=${carrier}"

    getApiKey match {
      case Right(apiKey) =>
        val request = sttp
          .get(url)
          .header("apiKey", apiKey)
          .response(asJson[List[MemberInsuranceRecord]])

        val ret = request.send.body

        ApiError
          .convertResponse[List[MemberInsuranceRecord], UnknownResponseError](
            ret,
            Some(url.toString)
          )

      case Left(e) => Left(e)
    }
  }
}
