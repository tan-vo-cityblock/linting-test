package cityblock.aggregators.jobs

import cityblock.member.service.models.PatientInfo.{Datasource, Patient}
import cityblock.streaming.jobs.RedoxSubscriber.RedoxMessage
import cityblock.streaming.jobs.common.ParsableData
import cityblock.transforms.redox.PrepareClinicalSummary
import cityblock.utilities.{EhrMedicalDataIO, Environment, ResultSigner}
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.{
  toBigQuerySCollection,
  toBigQueryScioContext,
  CREATE_IF_NEEDED,
  WRITE_APPEND
}
import com.spotify.scio.io.Tap
import com.spotify.scio.values.{SCollection, WindowOptions}
import com.spotify.scio.{ScioContext, ScioResult}
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time._

import scala.concurrent.Future

object ClinicalSummaryRefresher extends EhrMedicalDataIO with ParsableData {
  @BigQueryType.toTable
  case class ClinicalSummaryRefreshMessage(
    patientId: String,
    externalId: String,
    datasource: String
  )

  case class ClinicalInformation(mrn: String)

  case class ClinicalInformationUpdateMessage(
    patientId: String,
    clinicalInformation: ClinicalInformation
  )

  import argonaut._
  import Argonaut._

  implicit def clinicalInformationCodecJson: CodecJson[ClinicalInformation] =
    casecodec1(ClinicalInformation.apply, ClinicalInformation.unapply)("mrn")

  implicit def clinicalInformationUpdateMessageCodecJson
    : CodecJson[ClinicalInformationUpdateMessage] =
    casecodec2(
      ClinicalInformationUpdateMessage.apply,
      ClinicalInformationUpdateMessage.unapply
    )("patientId", "clinicalInformation")

  def processAndPersistMessages[E](
    messages: SCollection[ClinicalSummaryRefreshMessage],
    medicalDataset: String,
    isStreaming: Boolean
  )(implicit environment: Environment): Unit = {
    val ccdQueryResponseMessages = processMessages(messages)
    val windowedMessages = ccdQueryResponseMessages
      .withFixedWindows(
        Duration.standardSeconds(1),
        Duration.ZERO,
        WindowOptions(
          Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
          AccumulationMode.DISCARDING_FIRED_PANES,
          Duration.standardDays(7)
        )
      )

    saveRawQueryResponsesToBigQuery(windowedMessages)
    persistCcdResponse(windowedMessages, medicalDataset, isStreaming)
  }

  def persistCcdResponse(
    ccdQueryResponseMessages: SCollection[RedoxMessage],
    medicalDataset: String,
    isStreaming: Boolean
  )(implicit environment: Environment): Unit = {
    val parsedPatientMedicalData = parsePatientMedicalData(
      ccdQueryResponseMessages,
      isStreaming
    )
    aggregateAndPersistPatientMedicalData(
      patientMedicalDataLists = parsedPatientMedicalData,
      medicalDataset = medicalDataset
    )
  }

  def processMessages(
    messages: SCollection[ClinicalSummaryRefreshMessage]
  )(implicit environment: Environment): SCollection[RedoxMessage] =
    messages
      .flatMap(message => {
        val patientId = message.patientId
        val patientDatasource = message.datasource
        val externalId = message.externalId

        val ccd = PrepareClinicalSummary.pullClinicalSummary(externalId)
        ccd match {
          case Some(body) =>
            logger.info(s"""SUCCESS IN CCD QUERY [externalId: $externalId]""")
            val ccdMessage =
              RedoxMessage(
                Patient(
                  Some(patientId),
                  externalId,
                  Datasource(patientDatasource)
                ),
                "PatientQueryResponse",
                body,
                Instant.now
              )
            Seq(ccdMessage)
          case None =>
            logger.info(s"""UNABLE TO PULL NON-NULL CCD QUERY [externalId: $externalId]""")
            Seq()
        }
      })

  def saveRawQueryResponsesToBigQuery(
    rows: SCollection[RedoxMessage]
  ): Future[Tap[RedoxMessage]] = {
    val tableName = "streaming.redox_messages"
    rows.saveAsTypedBigQuery(tableName, WRITE_APPEND, CREATE_IF_NEEDED)
  }

  def publishUpdatedClinicalInformation(
    ccdRefreshMessages: SCollection[ClinicalSummaryRefreshMessage],
    projectName: String,
    topicName: String
  )(implicit environment: Environment): Unit = {
    val clinicalInformationUpdateTopic =
      s"projects/$projectName/topics/$topicName"

    ccdRefreshMessages
      .flatMap(message => {
        val clinicalInformation = ClinicalInformation(mrn = message.externalId)
        val clinicalInformationUpdateMessage =
          ClinicalInformationUpdateMessage(
            message.patientId,
            clinicalInformation
          )

        val payload = clinicalInformationUpdateMessage.asJson.nospaces
        val hmac = ResultSigner.signResult(payload)

        Seq(
          (payload, Map("topic" -> "clinicalInformationUpdate", "hmac" -> hmac))
        )
      })
      .withName("publishClinicalInformationUpdateMessages")
      .saveAsPubsubWithAttributes[String](clinicalInformationUpdateTopic)
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[DataflowPipelineOptions](cmdlineArgs)
    implicit val environment: Environment = Environment(cmdlineArgs)
    val sc = ScioContext(opts)

    val isStreaming = opts.isStreaming //will be false for batch jobs.

    val medicalDataset: String = args.required("medicalDataset")
    val clinicalInfoUpdateTopic: String =
      args.required("clinicalInfoUpdateTopic")
    val redoxMessagesTable: String = args.required("redoxMessagesTable")

    val patientId: Option[String] = args.optional("patientId")
    val externalId: Option[String] = args.optional("externalId")
    val datasource: Option[String] = args.optional("sourceName")

    val patientsToRefresh: SCollection[ClinicalSummaryRefreshMessage] =
      (patientId, externalId, datasource) match {
        case (Some(patientId), Some(externalId), Some(datasource)) =>
          sc.parallelize(Seq(ClinicalSummaryRefreshMessage(patientId, externalId, datasource)))
        case _ =>
          // Grabs the last known patientId for a given externalId, source combination
          sc.typedBigQuery[ClinicalSummaryRefreshMessage](
            s"""
            |SELECT patientId, externalId, datasource
            | FROM (
            | SELECT DISTINCT
            | patient.patientId AS patientId,
            | patient.externalId AS externalId,
            | patient.source.name AS datasource,
            | MAX(insertedAt) OVER (PARTITION BY patient.externalId, patient.source.name) AS max_date,
            | insertedAt,
            | FROM `${environment.projectName}.streaming.$redoxMessagesTable`
            | WHERE REGEXP_CONTAINS(eventType, r"[sS]cheduling\\.[^cC]") AND
            |   patient.patientId IS NOT NULL AND
            |   (DATE_DIFF(CAST(SUBSTR(json_extract(payload, "$$.Visit.VisitDateTime"), 2, 10) AS DATE), CURRENT_DATE(), WEEK)
            |   IN (-0, -1, -2) OR
            |   DATE_DIFF(DATE(insertedAt), CURRENT_DATE(), DAY) IN (-0, -1))
            | )
            | WHERE insertedAt = max_date
            |""".stripMargin
          )
      }

    publishUpdatedClinicalInformation(
      patientsToRefresh,
      environment.projectName,
      clinicalInfoUpdateTopic
    )
    processAndPersistMessages(patientsToRefresh, medicalDataset, isStreaming)
    val scioResult: ScioResult = sc.close().waitUntilFinish()

    // TODO: Abstract out below to be DRY
    // throwing explicitly to fail Airflow DAGs that run this job if pipeline completes with non-DONE state.
    scioResult.state match {
      case PipelineResult.State.DONE =>
        logger.info("Batch job succeeded.")
      case PipelineResult.State.UNKNOWN if opts.getTemplateLocation.nonEmpty =>
        logger.info("Batch job is being run in template format")
      case _ =>
        val errorStr = "Batch job failed. See logs above."
        logger.error(errorStr)
        throw new Exception(errorStr)
    }
  }
}
