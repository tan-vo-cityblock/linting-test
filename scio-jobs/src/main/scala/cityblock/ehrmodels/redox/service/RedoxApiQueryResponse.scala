package cityblock.ehrmodels.redox.service

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cityblock.utilities.Loggable
import com.github.vitalsoftware.scalaredox.client.{
  ClientConfig,
  RedoxAuthorizationException,
  RedoxClient,
  RedoxErrorResponse,
  RedoxResponse,
  RedoxTokenManager
}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.safety.Whitelist
import play.api.libs.json.{JsError, Json, Reads, Writes}
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}

trait RedoxApiClient {
  implicit def system: ActorSystem
  implicit def materializer: ActorMaterializer = ActorMaterializer()(system)

  def httpClient(
    implicit materializer: ActorMaterializer
  ): StandaloneAhcWSClient = StandaloneAhcWSClient()(materializer)

  def createClient(
    conf: ClientConfig
  )(implicit materializer: ActorMaterializer): RedoxClient = {
    val tokenManager = new RedoxTokenManager(httpClient, conf.baseRestUri)
    new RedoxClient(conf, httpClient, tokenManager)
  }
}

trait RedoxHandleQuery {
  def createQuery[Q](query: String)(implicit writes: Writes[Q], reads: Reads[Q]): Q = {
    val trimmedQuery = query.stripMargin
    Json
      .parse(trimmedQuery)
      .validate[Q]
      .fold(
        invalid = err => throw new RuntimeException(JsError.toJson(JsError(err)).toString()),
        valid = identity
      )
  }
}

trait RedoxHandleResponse extends Loggable { this: RedoxApiClient =>
  def awaitResponse[R](
    fut: Future[RedoxResponse[R]],
    timeoutInSeconds: FiniteDuration = 20.seconds
  )(implicit externalId: String): Option[R] =
    try {
      val resp = Await.result(fut, timeoutInSeconds)
      if (resp.isError) {
        val concatenatedErrors = concatenateErrors(resp.getError)
        throwAuthException(resp.getError)
        logger.error(
          s"""ERROR [RedoxHandleResponse.awaitResponse] [externalId: $externalId]: $concatenatedErrors"""
        )
      }
      resp.asOpt
    } catch {
      case e: TimeoutException =>
        logger.error(
          s"""ERROR FUTURE TIMEOUT EXCEPTION [RedoxHandleResponse.awaitResponse] [externalId: $externalId]: ${e.getStackTrace
            .mkString(", ")}"""
        )
        None
    }

  def cleanHtml(rawHtml: String): Option[String] = {
    val rawDocument = Jsoup.parse(rawHtml)
    val documentSettings = new Document.OutputSettings().prettyPrint(false)
    val prettyPrintedDoc =
      rawDocument.outputSettings(documentSettings)
    prettyPrintedDoc.select("br").append("\\n").select("p").prepend("\\n\\n")
    val newLineConciseDoc = prettyPrintedDoc.html().replaceAll("\\\\n", "\n")
    val finalNote = Jsoup
      .clean(
        newLineConciseDoc,
        "",
        Whitelist.none(),
        documentSettings
      )
      // XML/HTML ampersand coding to '&'
      .replaceAll("&amp;", "&")
      // remove spaces after new lines for formatting in lists, etc
      .replaceAll("\n ", "\n")
      // add space after times with AM/PM
      .replaceAll("M ([A-Z]{3})([\\S^.])", "M $1 $2")
      // add space after '.'
      .replaceAll("\\.([A-Z])", ". $1")
      // spaces are sometimes missing after dates, if that's the case, add a space
      .replaceAll("([-\0-9][0-9]{2})([A-Za-z]{2})", "$1 $2")
      // we don't need header labels in ReasonForVisitText that get concatenated
      .replaceAll("ReasonComments", "")
      // add space before/after capitalized words ("Annual ExamFollow-upSinus Problem")
      .replaceAll("([A-Z][a-z-]+)([A-Z][a-z-]+)", "$1 $2")
    if (finalNote.trim.isEmpty) {
      None
    } else {
      Some(finalNote)
    }
  }

  private def throwAuthException(response: RedoxErrorResponse): Unit = {
    val authError = "Unauthorized, This source is not authorized"
    val doesErrMsgContainsAuthErr = {
      response.Errors.exists(errorMsg => errorMsg.Text.contains(authError))
    }
    if (doesErrMsgContainsAuthErr) {
      throw RedoxAuthorizationException(concatenateErrors(response))
    }
  }

  private def concatenateErrors(response: RedoxErrorResponse): String =
    response.Errors.map(_.Text).mkString(", ")
}

object RedoxApiQueryResponse extends RedoxHandleResponse with RedoxHandleQuery with RedoxApiClient {
  implicit override def system: ActorSystem = ActorSystem("redox")
  val redoxClient: RedoxClient = createClient(RedoxApiConfig.conf)
}
