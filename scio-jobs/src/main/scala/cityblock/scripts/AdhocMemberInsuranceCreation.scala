package cityblock.scripts

import cityblock.member.service.api.{Detail, Insurance, Plan}
import cityblock.member.service.io.MemberService.getApiKey
import cityblock.utilities.Environment
import com.softwaremill.sttp._
import com.softwaremill.sttp.circe._
import com.spotify.scio.ContextAndArgs
import purecsv.safe.CSVReader

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source

case class csvFile(patientId: String,
                   externalId: String,
                   spanDateStart: Option[String],
                   spanDateEnd: Option[String],
                   current: Option[Boolean],
                   lineOfBusiness: Option[String],
                   subLineOfBusiness: Option[String])

object AdhocMemberInsuranceCreation {

  def main(cmdlineArgs: Array[String]): Unit = {

    val (_, args) = ContextAndArgs(cmdlineArgs)

    val fileName: String = args.required("fileName")
    val insertMethod: String = args.required("insertMethod")

    require(
      insertMethod.toLowerCase() == "update" || insertMethod.toLowerCase() == "create",
      """
        |set `--insertMethod to either "update" or "create:
        |""".stripMargin
    )

    val file = Source.fromFile(fileName)
    val lines = file.getLines()

    val csvLines = lines.map(i => CSVReader[csvFile].readCSVFromString(i).head.get).toList

    val request = csvLines.groupBy(k => (k.patientId, k.externalId))

    val promises = request
      .map(
        entry => convertToRequest(entry)
      )
      .map {
        case (patientId, jsonPayload) if insertMethod.toLowerCase == "create" =>
          createInsuranceInMemberService(patientId = patientId, jsonPayload = jsonPayload)
        case (patientId, jsonPayload) if insertMethod.toLowerCase == "update" =>
          updateInsuranceInMemberService(patientId = patientId, jsonPayload = jsonPayload)
      }

    val promiseList = Future.sequence(promises)

    val listOfFullFilledPromises = Await.result(promiseList, Duration.Inf)

    listOfFullFilledPromises
      .collect { case Left(error) => error }
      .foreach(print(_))

  }

  def convertToRequest(entry: ((String, String), List[csvFile])): (String, Insurance) = {
    val (patientId, externalId) = entry._1
    val details = entry._2.map { csv =>
      Detail(
        spanDateStart = csv.spanDateStart,
        spanDateEnd = csv.spanDateEnd,
        lineOfBusiness = csv.lineOfBusiness,
        subLineOfBusiness = csv.subLineOfBusiness
      )
    }

    /*
     * If at least one true exists, then it is actively current.
     * If all are false, then it is not active
     * If they are all empty then it is none
     */

    val currentList = entry._2
      .flatMap(csv => csv.current)

    val current =
      if (currentList.isEmpty) { None } else { Some(currentList.reduce((a, b) => a || b)) }

    val plans = Plan(externalId = externalId, current = current, details = details, rank = None)

    (patientId, Insurance(carrier = "emblem", plans = List(plans)))
  }

  def createInsuranceInMemberService(jsonPayload: Insurance,
                                     patientId: String): Future[Either[String, String]] =
    Future {
      implicit val env = Environment.Prod
      implicit val backend = env.backends.sttpBackend

      val url = uri"https://cbh-member-service-prod.appspot.com/1.0/members/${patientId}/insurance"

      getApiKey match {
        case Right(apiKey) =>
          val request = sttp
            .post(url)
            .header("apiKey", apiKey)
            .body(jsonPayload)
            .response(asString)

          request.send.body
        case _ =>
          Left(
            s"Member: ${patientId} unable to insert externalId"
          )
      }
    }

  def updateInsuranceInMemberService(jsonPayload: Insurance,
                                     patientId: String): Future[Either[String, String]] =
    Future {
      implicit val env = Environment.Prod
      implicit val backend = okhttp.OkHttpSyncBackend()

      val url = uri"https://cbh-member-service-prod.appspot.com/1.0/members/${patientId}/insurance"

      getApiKey match {
        case Right(apiKey) =>
          val request = sttp
            .patch(url)
            .header("apiKey", apiKey)
            .body(jsonPayload)
            .response(asString)

          request.send.body
        case _ =>
          Left(
            s"Member: ${patientId} unable to insert externalId"
          )
      }
    }
}
