package cityblock.importers.generic

import cityblock.importers.generic.config.{CsvFileConfig, PartnerInputFileConfig}
import cityblock.member.service.models.PatientInfo.{Datasource, Patient}
import cityblock.transforms.Transform
import cityblock.utilities.{Environment, Loggable, ScioUtils}
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.{
  WRITE_APPEND,
  WRITE_TRUNCATE
}
import org.joda.time.LocalDate

import scala.concurrent.Future

object LoadToSilver extends Loggable {
  val parseErrorsTableName = "ParseErrors"

  // scalastyle:off parameter.number
  def runLoadToSilverJob(
    args: Array[String],
    inputConfigs: List[PartnerInputFileConfig],
    partnerName: String,
    deliveryDate: String,
    outputProject: String,
    outputDataset: String,
    shard: LocalDate,
    index: Int,
    writeDisposition: Option[String]
  )(implicit environment: Environment): Future[Tap[FailedParseContainer.FailedParse]] =
    ScioUtils
      .runJob(s"loadtosilver-$partnerName-$deliveryDate-$index", args) { sc =>
        {
          // Load the member index if it's needed by any of the configs.
          val memberIndex =
            maybeLoadMemberIndex(sc, inputConfigs)

          // Load and parse data into key-value maps. Also separately record all parse failures in
          // a separate collection.
          val parseResults = inputConfigs.map { config =>
            val parseResult = config match {
              case c: CsvFileConfig =>
                loadFromCsvFile(sc, c, deliveryDate)
              case _ =>
                throw new IllegalArgumentException("Unsupported config type")
            }
            (config, parseResult)
          }

          // Convert the key-value maps to complete TableRows.
          val tableRowCollections = parseResults.map {
            case (config, parsedInput) =>
              (config, toTableRows(config, parsedInput.validRows, memberIndex))
          }

          // Write the collections of TableRows to their respective tables in BigQuery.
          tableRowCollections.foreach {
            case (config, tableRows) =>
              writeToBigQuery(config,
                              tableRows,
                              outputProject,
                              outputDataset,
                              shard,
                              writeDisposition)
          }

          // Collect all the rows that couldn't be parsed into a single collection and mark it to
          // be materialized for the final job that writes all the errors to BigQuery.
          SCollection
            .unionAll(parseResults.map {
              case (_, parseResult) => parseResult.invalidRows
            })
            .materialize
        }
      }
      ._2

  def runWriteErrorsToBigQueryJob(
    args: Array[String],
    invalidRowFutures: Array[Future[Tap[FailedParseContainer.FailedParse]]],
    partnerName: String,
    deliveryDate: String,
    outputProject: String,
    outputDataset: String,
    shard: LocalDate
  )(implicit environment: Environment): Unit =
    ScioUtils.runJob(s"loadtosilver-$partnerName-$deliveryDate-errors", args) { sc =>
      {
        val allInvalidRows =
          SCollection.unionAll(invalidRowFutures.map(ScioUtils.waitAndOpen(_, sc)))

        Transform.persist(
          allInvalidRows,
          outputProject,
          outputDataset,
          parseErrorsTableName,
          shard,
          WRITE_TRUNCATE,
          CREATE_IF_NEEDED
        )
      }
    }

  private def maybeLoadMemberIndex(sc: ScioContext, inputConfigs: List[PartnerInputFileConfig])(
    implicit environment: Environment): Option[SCollection[(String, Patient)]] = {
    import cityblock.member.service.io._
    inputConfigs
      .find(_.indexJoinField.isDefined)
      .map(config => {
        val carrier: String = config.datasource.getOrElse(config.partnerName)
        sc.readInsurancePatients(Option(carrier))
      })
  }

  private def loadFromCsvFile(
    sc: ScioContext,
    config: CsvFileConfig,
    deliveryDate: String
  ): ParseResult = {
    val filePath = config.getInputFilePath(deliveryDate)

    val tableName = config.tableName

    val parsedLines = sc
      .withName(s"Load $filePath")
      .textFile(filePath)
      .withName(s"$tableName: Filter out header row")
      .filter(_ != config.header)
      .withName(s"$tableName: Parse ${config.fileType} file")
      .map { row =>
        val stripped = row.replace("\u0000", "")
        config.parseLine(stripped)
      }

    val validRows = parsedLines.collect {
      case Right(mapping) => mapping
    }
    val invalidRows = parsedLines.collect {
      case Left(failedParse) => failedParse
    }

    ParseResult(validRows, invalidRows)
  }

  private def toTableRows(
    config: PartnerInputFileConfig,
    parsedRows: SCollection[Map[String, String]],
    memberIndex: Option[SCollection[(String, Patient)]]
  ): SCollection[TableRow] = {
    val tableName = config.tableName
    val tableRows =
      if (config.indexJoinField.isDefined && memberIndex.isDefined) {
        val joinField = config.indexJoinField.get
        parsedRows
          .withName(s"$tableName: Key by $joinField")
          .keyBy(row => {
            val joinValue: String = row(joinField)
            if (config.dataType.contains("HealthRiskAssessment")) {
              if (joinValue.length == 9) joinValue + "01" else joinValue
            } else {
              joinValue
            }
          })
          .withName(s"$tableName: Join with member index")
          .leftOuterJoin(memberIndex.get)
          .withName(s"$tableName: Convert to TableRow")
          .map {
            case (_, (mapping, maybe)) =>
              val member = maybe.getOrElse(
                Patient(None, mapping(joinField), Datasource(config.partnerName, None))
              )
              config.toTableRow(mapping, Some(member))
          }
      } else {
        parsedRows
          .withName(s"$tableName: Convert to TableRow")
          .map(config.toTableRow(_, None))
      }

    tableRows
  }

  private def writeToBigQuery(
    config: PartnerInputFileConfig,
    tableRows: SCollection[TableRow],
    outputProject: String,
    outputDataset: String,
    shard: LocalDate,
    wd: Option[String]
  ): Unit = {
    val writeDisposition: WriteDisposition = wd.getOrElse("WriteTruncate") match {
      case "WriteAppend"   => WRITE_APPEND
      case "WriteTruncate" => WRITE_TRUNCATE
    }

    val tableName = config.tableName
    val tableSpec =
      s"$outputProject:$outputDataset.${Transform.shardName(tableName, shard)}"
    tableRows.internal.apply(
      s"$tableName: Write to BigQuery",
      BigQueryIO
        .writeTableRows()
        .to(tableSpec)
        .withSchema(config.tableSchema)
        .withCreateDisposition(CREATE_IF_NEEDED)
        .withWriteDisposition(writeDisposition)
    )
  }
}
