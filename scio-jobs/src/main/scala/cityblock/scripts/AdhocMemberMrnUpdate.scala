package cityblock.scripts
import cityblock.member.service.io.MemberService
import cityblock.utilities.{Environment, Loggable}
import com.softwaremill.sttp.circe.asJson
import com.softwaremill.sttp.sttp
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType
import com.softwaremill.sttp._
import io.circe.Encoder
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import com.softwaremill.sttp.circe._
import cityblock.member.service.api._
import io.circe.syntax._

object AdhocMemberMrnUpdateModels {
  @BigQueryType.toTable
  case class MemberMrnUpdate(memberId: String, mrn: String, name: String)

  case class MemberMrnUpdateBody(id: String, name: String)

  case class MemberMrnUpdateResponse(id: String, name: String)
}

object AdhocMemberMrnUpdate extends Loggable {
  import AdhocMemberMrnUpdateModels._

  def main(cmdlineArgs: Array[String]): Unit = {

    implicit val environment: Environment = Environment(cmdlineArgs)
    val (sc, _) = ContextAndArgs(cmdlineArgs)

    val memberMrnUpdates = sc.typedBigQuery[MemberMrnUpdate](
      s"""
         |#standardsql
         |SELECT m.id as memberId, mrn.mrn, mrn.name
         |FROM `cbh-db-mirror-prod.member_index_mirror.mrn` mrn
         |JOIN `cbh-db-mirror-prod.member_index_mirror.member` m
         |ON m.mrnId = mrn.id
         |WHERE mrn.mrn LIKE '% ' OR mrn.mrn LIKE ' %'
         |""".stripMargin
    )

    memberMrnUpdates.map { callMemberServiceToUpdateMemberMrn }

    sc.close().waitUntilFinish()
  }

  private def callMemberServiceToUpdateMemberMrn(
    memberMrnUpdate: MemberMrnUpdate
  )(
    implicit environment: Environment
  ) = {
    implicit val deleteEncoder: Encoder[MemberMrnUpdateResponse] =
      deriveEncoder[MemberMrnUpdateResponse]
    implicit val backend = environment.backends.sttpBackend

    val MemberMrnUpdate(memberId, mrn, name) = memberMrnUpdate
    val updateMrnBody = MemberMrnUpdateBody(mrn.trim, name.trim)

    val url = uri"${MemberService.memberEndpoint}/members/$memberId/mrn"

    logger.info(s"Update member mrn [memberId: $memberId, body: $updateMrnBody]")

    MemberService.getApiKey match {
      case Right(apiKey) =>
        val request = sttp
          .post(url)
          .header("apiKey", apiKey)
          .body(updateMrnBody)
          .response(asJson[MemberMrnUpdateResponse])

        val ret = request.send.body
        logger.info(s"Received update member mrn response [memberId: $memberId, ret: $ret]")

      case Left(e) => Left(e)
    }
  }
}
