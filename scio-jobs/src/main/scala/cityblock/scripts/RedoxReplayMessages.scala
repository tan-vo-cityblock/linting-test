package cityblock.scripts

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions

object RedoxReplayMessages {

  @BigQueryType.toTable
  case class Message(eventType: String, payload: String)

  // NOTE: Craft the BQ query below to pull the messages that were lost
  def query() =
    s"""
    |SELECT
    |   payload, eventType
    |FROM `cityblock-data.streaming.redox_messages`
    |WHERE patient.patientId is null
    |AND patient.source.name = 'carefirst'  
    |AND insertedAt > '2021-05-13 16:15:33.869 UTC'
    |AND (eventType = 'PatientAdmin.Discharge' OR eventType = 'PatientAdmin.Arrival')
    |ORDER BY insertedAt DESC
    |LIMIT 1
    """.stripMargin

  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[GcpOptions](cmdlineArgs)
    val sc = ScioContext(opts)
    val topic = args.required("topic")

    sc.typedBigQuery[Message](query())
      .map { message =>
        // `type` attribute necessary to mimic RedoxPublisher
        // https://github.com/cityblock/redox_publisher
        (message.payload, Map("type" -> message.eventType))
      }
      .saveAsPubsubWithAttributes[String](topic)
    sc.close()
  }

}
