package cityblock.scripts

import cityblock.member.service.io.MemberService
import cityblock.utilities.{Environment, Loggable}
import com.softwaremill.sttp.circe.asJson
import com.softwaremill.sttp.sttp
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType
import com.softwaremill.sttp._
import com.softwaremill.sttp.circe._
import cityblock.member.service.api._
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import io.circe.syntax._

object AdhocMemberDeletionModels {
  @BigQueryType.toTable
  case class MemberIdForDelete(id: String)

  case class MemberDeleteResponse(
    deleted: Boolean,
    deletedAt: Option[String],
    deletedReason: Option[String],
    deletedBy: Option[String]
  )
}

object AdhocMemberDeletion extends Loggable {
  import AdhocMemberDeletionModels._

  def main(cmdlineArgs: Array[String]): Unit = {

    implicit val environment: Environment = Environment(cmdlineArgs)
    val (sc, _) = ContextAndArgs(cmdlineArgs)

    val memberIdsForZendesk = sc.typedBigQuery[MemberIdForDelete](
      s"""
         |#standardsql
         |SELECT id FROM `cbh-db-mirror-prod.member_index_mirror.member` WHERE mrnId IS NULL AND createdAt > "2020-11-22" AND partnerId = 1
         |""".stripMargin
    )

    memberIdsForZendesk.map { memberId =>
      callMemberServiceToDeleteMember(memberId.id)
    }

    sc.close().waitUntilFinish()
  }

  private def callMemberServiceToDeleteMember(
    memberId: String
  )(implicit environment: Environment) = {
    implicit val backend = environment.backends.sttpBackend

    val url = uri"${MemberService.memberEndpoint}/members/$memberId"

    logger.info(s"Deleting member [memberId: $memberId]")

    MemberService.getApiKey match {
      case Right(apiKey) =>
        val request = sttp
          .delete(url)
          .header("apiKey", apiKey)
          .response(asJson[MemberDeleteResponse])

        val ret = request.send.body
        logger.info(s"Received delete member response [memberId: $memberId, ret: $ret]")

      case Left(e) => Left(e)
    }
  }

}
