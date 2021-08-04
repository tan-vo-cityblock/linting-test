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

object AdhocMemberMrnDeletionModels {
  @BigQueryType.toTable
  case class MemberIdForMrnDelete(id: String, mrn: String)

  case class MemberMrnDeleteResponse(deleted: Boolean)
}

object AdhocMemberMrnDeletion extends Loggable {
  import AdhocMemberMrnDeletionModels._

  def main(cmdlineArgs: Array[String]): Unit = {

    implicit val environment: Environment = Environment(cmdlineArgs)
    val (sc, _) = ContextAndArgs(cmdlineArgs)

    val memberIdsForMrnDeletion = sc.typedBigQuery[MemberIdForMrnDelete](
      s"""
         |#standardsql
         |SELECT
         |  m.id,
         |  mrn.mrn
         |FROM
         |  `emblem-data.cohort8.virtual_members_transition` vm
         |JOIN
         |  `cbh-db-mirror-prod.member_index_mirror.member` m
         |ON
         |  m.id = vm.memberId
         |JOIN
         |  `cbh-db-mirror-prod.member_index_mirror.mrn` mrn
         |ON
         |  m.mrnId = mrn.id
         |ORDER BY 1
         |LIMIT 1500 OFFSET 1
         |""".stripMargin
    )

    memberIdsForMrnDeletion.map { memberId =>
      callMemberServiceToDeleteMemberMrn(memberId.id)
    }

    sc.close().waitUntilFinish()
  }

  private def callMemberServiceToDeleteMemberMrn(
    memberId: String
  )(
    implicit environment: Environment
  ) = {
    implicit val deleteEncoder: Encoder[MemberMrnDeleteResponse] =
      deriveEncoder[MemberMrnDeleteResponse]
    implicit val backend = environment.backends.sttpBackend

    val url = uri"${MemberService.memberEndpoint}/members/$memberId/mrn"

    logger.info(s"Deleting member mrn [memberId: $memberId]")

    MemberService.getApiKey match {
      case Right(apiKey) =>
        val request = sttp
          .delete(url)
          .header("apiKey", apiKey)
          .response(asJson[MemberMrnDeleteResponse])

        val ret = request.send.body
        logger.info(s"Received delete member mrn response [memberId: $memberId, ret: $ret]")

      case Left(e) => Left(e)
    }
  }

}
