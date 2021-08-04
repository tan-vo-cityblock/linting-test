package cityblock.scripts

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions

object ElationReplayMessages {

  @BigQueryType.toTable
  case class Message(eventType: String, payload: String, messageId: String)

  def query(date: String) =
    s"""
         |SELECT
         |  payload, messageId, eventType
         |FROM
         |  `cityblock-data.streaming.elation_messages`
         |WHERE
         |  patient.patientId is null AND
         |  insertedAt > ${date}
         |""".stripMargin

  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[GcpOptions](cmdlineArgs)
    val sc = ScioContext(opts)
    val date = args.required("date")

    sc.typedBigQuery[Message](query(date = date))
      .map { message =>
        val action = message.eventType.split("_")(1)
        val resource = message.eventType.split("_")(0)
        (message.payload,
         Map("action" -> action, "resource" -> resource, "messageId" -> message.messageId))
      }
      .saveAsPubsubWithAttributes[String](
        "projects/cityblock-data/topics/elationEvents"
      )
    sc.close()
  }

}
