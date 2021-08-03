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
import io.circe.Encoder
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import io.circe.syntax._

object AdhocMemberCohortReassignmentModels {
  @BigQueryType.toTable
  case class MemberIdForCohortReassignment(memberId: String, cohortName: String)

  case class CohortReassignmentBody(cohortName: String)

  case class CohortReassignmentResponse(memberId: String)
}

object AdhocMemberCohortReassignment extends Loggable {
  import AdhocMemberCohortReassignmentModels._

  def main(cmdlineArgs: Array[String]): Unit = {

    implicit val environment: Environment = Environment(cmdlineArgs)
    val (sc, _) = ContextAndArgs(cmdlineArgs)

    val memberIdsForMrnDeletion = sc.typedBigQuery[MemberIdForCohortReassignment](
      s"""
         |#standardsql
         |SELECT
         |  memberId,
         |  cohortName
         |FROM
         |  `emblem-data.cohort8.virtual_members_transition`
         |ORDER BY 1
         |LIMIT 1500
         |OFFSET 1
         |""".stripMargin
    )

    memberIdsForMrnDeletion.map { callMemberServiceToReassignCohort }

    sc.close().waitUntilFinish()
  }

  private def callMemberServiceToReassignCohort(
    member: MemberIdForCohortReassignment
  )(
    implicit environment: Environment
  ) = {
    implicit val deleteEncoder: Encoder[CohortReassignmentResponse] =
      deriveEncoder[CohortReassignmentResponse]
    implicit val backend = environment.backends.sttpBackend

    val MemberIdForCohortReassignment(memberId, cohortName) = member
    val url = uri"${MemberService.memberEndpoint}/members/$memberId/cohort"
    val cohortReassignmentBody = CohortReassignmentBody(cohortName)

    logger.info(s"Reassigning member to new cohort [memberId: $memberId, cohort: $cohortName]")

    MemberService.getApiKey match {
      case Right(apiKey) =>
        val request = sttp
          .post(url)
          .header("apiKey", apiKey)
          .body(cohortReassignmentBody)
          .response(asJson[CohortReassignmentResponse])

        val ret = request.send.body
        logger.info(s"Cohort reassignment response [memberId: $memberId, ret: $ret]")

      case Left(e) => Left(e)
    }
  }

}
