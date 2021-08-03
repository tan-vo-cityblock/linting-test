package cityblock.scripts

import cityblock.member.service.io.MemberService
import cityblock.utilities.{Environment, Loggable}
import com.softwaremill.sttp.{sttp, _}
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.bigquery.types.BigQueryType
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object AdhocMemberResyncModels {
  @BigQueryType.toTable
  case class MemberIdForResync(memberId: String)
}

object AdhocMemberResync extends Loggable {
  import AdhocMemberResyncModels._

  def main(cmdlineArgs: Array[String]): Unit = {

    implicit val environment: Environment = Environment(cmdlineArgs)

    val (opts, _) = ScioContext.parseArguments[DataflowPipelineOptions](cmdlineArgs)

    val project = opts.getProject
    val bq = BigQuery(project = project)

    val memberIdsForResync = bq.getTypedRows[MemberIdForResync](
      s"""
         |#standardsql
         |WITH
         |elation AS (
         |  SELECT patient_id, member_id, rank
         |  FROM
         |    `cbh-db-mirror-prod.elation_mirror.patient_insurance_latest`),
         |
         |commons AS (
         |  SELECT id, memberId, nmi, insurance AS insuranceName
         |  FROM
         |    `cbh-db-mirror-prod.commons_mirror.patient`),
         |
         |ms AS (
         |  SELECT
         |    mi.memberId AS msMemberId,
         |    mi.externalId AS msExternalId,
         |    mrn.mrn AS msMrn,
         |    mi.CURRENT AS msCurrent
         |  FROM
         |    `cbh-db-mirror-prod.member_index_mirror.member_insurance` AS mi
         |  INNER JOIN
         |    `cbh-db-mirror-prod.member_index_mirror.member` AS member
         |  ON
         |    mi.memberId = member.id
         |  INNER JOIN
         |    `cbh-db-mirror-prod.member_index_mirror.mrn` AS mrn
         |  ON
         |    member.mrnid = mrn.id
         |  WHERE
         |    mi.CURRENT IS TRUE
         |    AND mi.datasourceId NOT IN (6, 7, 8, 25, 28, 29, 26, 27, 30, 31)
         |    AND mi.deletedAt IS NULL
         |    AND member.deletedAt IS NULL ),
         |
         |state AS (
         |  SELECT
         |    patient_state.currentState,
         |    patient_state.patientId
         |  FROM
         |    `cbh-db-mirror-prod.commons_mirror.patient_state` AS patient_state
         |  WHERE
         |    deletedAt IS NULL)
         |
         |SELECT DISTINCT commons.id as memberId
         |FROM commons
         |JOIN ms ON commons.id = ms.msMemberId
         |JOIN elation ON CAST(ms.msMrn AS string) = CAST(elation.patient_id AS string)
         |JOIN state ON state.patientId = commons.id
         |WHERE
         |  TRIM(ms.msExternalId) <> TRIM(elation.member_Id)
         |  AND state.currentState <> 'disenrolled'
         |""".stripMargin
    )

    val promises = memberIdsForResync.map { memberIdForResync =>
      callMemberServiceToResyncMember(memberIdForResync.memberId)
    }

    val promiseList: Future[Iterator[Either[String, String]]] = Future.sequence(promises)

    val listOfFullFilledPromises: Iterator[Either[String, String]] =
      Await.result(promiseList, Duration.Inf)

    listOfFullFilledPromises
      .collect { case Left(error) => error }
      .foreach(print(_))

  }

  private def callMemberServiceToResyncMember(memberId: String)(
    implicit environment: Environment): Future[Either[String, String]] =
    Future {
      {

        implicit val backend = environment.backends.sttpBackend

        val url = uri"${MemberService.memberEndpoint}/members/resync/$memberId"

        MemberService.getApiKey match {
          case Right(apiKey) =>
            val request = sttp
              .get(url)
              .header("apiKey", apiKey)
              .response(asString)

            request.send.body

          case Left(e) => Left(e.toString)
        }
      }
    }

}
