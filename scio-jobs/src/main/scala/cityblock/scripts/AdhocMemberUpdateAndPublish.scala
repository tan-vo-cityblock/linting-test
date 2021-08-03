package cityblock.scripts

import cityblock.member.service.io.MemberService
import cityblock.utilities.{Environment, Loggable}
import com.softwaremill.sttp.sttp
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType
import com.softwaremill.sttp._
import com.softwaremill.sttp.circe._
import cityblock.member.service.api._
import com.spotify.scio.bigquery.client.BigQuery
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import io.circe.syntax._
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object AdhocMemberUpdateAndPublishModels {
  @BigQueryType.toTable
  case class MemberIdForUpdateAndPublish(memberId: String)

  case class MemberCreateAndPublishRequestLite(
    marketId: String,
    partnerId: String,
    clinicId: String
  )
}

object AdhocMemberUpdateAndPublish extends Loggable {
  import AdhocMemberUpdateAndPublishModels._

  def main(cmdlineArgs: Array[String]): Unit = {

    implicit val environment: Environment = Environment(cmdlineArgs)
    val (opts, _) = ScioContext.parseArguments[DataflowPipelineOptions](cmdlineArgs)

    val project = opts.getProject
    val bq = BigQuery(project = project)

    val memberIdsForCohortReassignment = bq.getTypedRows[MemberIdForUpdateAndPublish](
      s"""
         |#standardsql
         |SELECT
         |  memberId
         |FROM
         |  `emblem-data.cohort9.virtual_member_transition`
         |ORDER BY 1
         |LIMIT 1
         |""".stripMargin
    )

    val sequencedPromise = Future.sequence(
      memberIdsForCohortReassignment.map { callMemberServiceToUpdateAndPublish }
    )

    val fulfilledPromises = Await.result(sequencedPromise, Duration.Inf)

    fulfilledPromises
      .collect { case Left(error) => error }
      .foreach(print(_))
  }

  private def callMemberServiceToUpdateAndPublish(
    member: MemberIdForUpdateAndPublish
  )(
    implicit environment: Environment
  ): Future[Either[String, String]] =
    Future {
      implicit val backend = environment.backends.sttpBackend

      val MemberIdForUpdateAndPublish(memberId) = member
      val url = uri"${MemberService.memberEndpoint}/members/$memberId/updateAndPublish"

      logger.info(s"Updating member for market, partner, clinic info [memberId: $memberId]")

      val updateAndPublishBody = MemberCreateAndPublishRequestLite(
        marketId = "3d8e4e1d-bdb5-4676-9e25-9b6fa248101f",
        partnerId = "5749b36b-8b04-4323-9a7d-fc99b242bf7d",
        clinicId = "b250a071-b261-4af7-90e2-67d2e22789ac"
      )

      MemberService.getApiKey match {
        case Right(apiKey) =>
          val request = sttp
            .post(url)
            .header("apiKey", apiKey)
            .body(updateAndPublishBody)
            .response(asString)

          val ret = request.send.body
          logger.info(
            s"Updated member market, partner, clinic info [memberId: $memberId, ret: $ret]")
          ret

        case Left(e) => Left(e.toString)
      }
    }

}
