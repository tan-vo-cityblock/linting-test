package cityblock.importers.emblem
import cityblock.streaming.jobs.RedoxSubscriber.RedoxMessage
import cityblock.streaming.jobs.common.ParsableData
import cityblock.utilities.{EhrMedicalDataIO, Environment}
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.values.SCollection
import com.spotify.scio.bigquery.WRITE_TRUNCATE
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions

object ReprocessStreamingMessages extends EhrMedicalDataIO with ParsableData {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, _) = ScioContext.parseArguments[DataflowPipelineOptions](cmdlineArgs)
    val sc = ScioContext(opts)
    val projectName = opts.getProject

    val queryString =
      s"""
         |SELECT * FROM `$projectName.streaming.redox_messages` WHERE eventType in ('PatientQueryResponse','Results.New','Notes.New')
       """.stripMargin

    val isStreaming = opts.isStreaming
    implicit val environment: Environment = Environment(cmdlineArgs)

    reprocessStreamingMessages(sc, queryString, isStreaming)
    sc.close().waitUntilDone()
  }

  private def reprocessStreamingMessages(
    sc: ScioContext,
    queryString: String,
    isStreaming: Boolean)(implicit environment: Environment): Unit = {
    val streamingMessages = sc.typedBigQuery[RedoxMessage](queryString)

    reprocessCCDQueryResponseMessages(streamingMessages, isStreaming)
    reprocessResultsMessages(streamingMessages)
  }

  private def reprocessCCDQueryResponseMessages(
    messages: SCollection[RedoxMessage],
    isStreaming: Boolean)(implicit environment: Environment): Unit = {
    val parsedPatientMedicalData = parsePatientMedicalData(
      messages.filter(m => m.eventType == "PatientQueryResponse"),
      isStreaming
    )
    aggregateAndPersistPatientMedicalData(
      patientMedicalDataLists = parsedPatientMedicalData,
      medicalDataset = "medical",
      writeDisposition = WRITE_TRUNCATE,
    )
  }

  private def reprocessResultsMessages(
    unfilteredMessages: SCollection[RedoxMessage]
  )(implicit environment: Environment): Unit = {
    val parsedPatientOrders = filterAndParsePatientOrders(unfilteredMessages)
    persistPatientMedicalData(
      parsedPatientOrders,
      "medical.patient_orders",
      WRITE_TRUNCATE
    )
  }
}
