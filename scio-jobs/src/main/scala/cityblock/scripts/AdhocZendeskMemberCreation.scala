package cityblock.scripts

import cityblock.member.service.io.MemberService
import cityblock.scripts.AdhocZendeskMemberModels.{CreateZendeskRequestBody, MemberIdForZendesk}
import cityblock.utilities.{Environment, Loggable}
import com.softwaremill.sttp.circe._
import com.softwaremill.sttp.{sttp, _}
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType
import io.circe.generic.auto._

object AdhocZendeskMemberModels {
  @BigQueryType.toTable
  case class MemberIdForZendesk(id: String)

  case class CreateZendeskRequestBody(
    organizationName: String,
    primary_hub: Option[String],
    covid_risk_level: Option[String],
    care_model: Option[String],
    primary_chp: Option[String],
    pcp: Option[String]
  )
}

object AdhocZendeskMemberCreation extends Loggable {

  def main(cmdlineArgs: Array[String]): Unit = {

    implicit val environment: Environment = Environment(cmdlineArgs)
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // NOTE: Modify the query below to pull members that are missing Zendesk IDs
    val memberIdsForZendesk = sc.typedBigQuery[MemberIdForZendesk](
      s"""
      |SELECT * FROM `cbh-db-mirror-prod.member_index_mirror.member`
      |WHERE zendeskId IS NULL
      |AND createdAt > "2021-05-26"
      |AND partnerId = 9
      |""".stripMargin
    )

    memberIdsForZendesk.map { memberId =>
      callMemberServiceForZendesk(memberId.id)
    }

    sc.close().waitUntilFinish()
  }

  private def callMemberServiceForZendesk(
    memberId: String
  )(
    implicit environment: Environment
  ) = {
    implicit val backend = environment.backends.sttpBackend

    // NOTE: Modify `organizationName` according to `organizationNameToIdMapping`
    // in services/member_service/src/util/zendesk. This is the organization
    // name in Zendesk. The rest of the params are Zendesk tags. Ask Joe Vita
    // if you need to add any.
    val zendeskBody = CreateZendeskRequestBody(
      organizationName = "North Carolina",
      primary_hub = None,
      covid_risk_level = None,
      care_model = None,
      primary_chp = None,
      pcp = None
    )

    val url = uri"${MemberService.memberEndpoint}/members/$memberId/zendesk"

    logger.info(s"Sending zendesk create request for [memberId: $memberId, body: $zendeskBody]")

    MemberService.getApiKey match {
      case Right(apiKey) =>
        val request = sttp
          .post(url)
          .header("apiKey", apiKey)
          .body(zendeskBody)
          .response(asString)

        val ret = request.send.body
        logger.info(s"Received zendesk create response [memberId: $memberId, ret: $ret]")

      case Left(e) => Left(e)
    }
  }

}
