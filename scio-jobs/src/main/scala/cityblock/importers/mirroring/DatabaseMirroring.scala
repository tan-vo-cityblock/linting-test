package cityblock.importers.mirroring

import cityblock.importers.generic.FailedParseContainer.FailedParse
import cityblock.importers.generic.config.BigQueryField
import cityblock.importers.mirroring.DatabaseMirroringRunner.DatabaseMirroringConfig
import cityblock.importers.mirroring.config.TableSchemaConfig
import cityblock.transforms.Transform
import cityblock.utilities.{Environment, Loggable, ScioUtils}
import com.google.cloud.storage.Storage
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.values.{DistCache, SCollection}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.{
  WRITE_APPEND,
  WRITE_TRUNCATE
}
import resource.managed

import scala.io.Source

object DatabaseMirroring extends Loggable {

  def runMirroringJob(
    args: Array[String],
    inputConfig: TableSchemaConfig,
    outputConfig: DatabaseMirroringConfig,
    gcsStorageConnector: Storage
  )(implicit environment: Environment): Unit = {

    val databaseNameForJob = inputConfig.databaseName.replace('_', '-')
    val DatabaseMirroringConfig(
      outputProject,
      outputDataset,
      shard
    ) = outputConfig

    val (result, _) = ScioUtils
      .runJob(s"mirroring-$databaseNameForJob", args) { sc =>
        inputConfig.tableNames.foreach { tableName: String =>
          // Load schema file into a List[BigQueryFields]
          val dcSchemaFile: DistCache[List[String]] = sc
            .distCache(inputConfig.schemaFileLocation(tableName)) { file =>
              managed(Source.fromFile(file)).acquireAndGet(_.getLines().toList)
            }

          val schemaFields: List[BigQueryField] = dcSchemaFile()
            .filter(_.trim.nonEmpty)
            .map(inputConfig.parseSchemaLine)

          // Parse input data using schema file
          val shardDate = shard.toString("YYYYMMdd")
          val parsedDataLines = sc
            .withName(s"Load export from $tableName on $shardDate")
            .textFile(inputConfig.dataFileLocation(tableName))
            .withName(s"Parse export from $tableName into map")
            .map(inputConfig.parseLine(tableName, _, schemaFields))

          // Convert parsed data into table rows
          val validTableRows: SCollection[TableRow] = parsedDataLines
            .collect { case Right(mapping) => mapping }
            .map {
              _.foldLeft[TableRow](new TableRow()) {
                case (tableRow, (fieldName, value)) =>
                  tableRow.set(fieldName, value)
              }
            }

          val invalidTableRows: SCollection[FailedParse] = parsedDataLines
            .collect { case Left(failedParse) => failedParse }

          // Write out save-able lines to BQ
          val shardName = Transform.shardName(tableName, shard)
          val tableSpec = s"$outputProject:$outputDataset.$shardName"
          validTableRows.internal.apply(
            s"$shardName: Write to BigQuery",
            BigQueryIO
              .writeTableRows()
              .to(tableSpec)
              .withSchema(inputConfig.getTableSchema(schemaFields))
              .withCreateDisposition(CREATE_IF_NEEDED)
              .withWriteDisposition(WRITE_TRUNCATE)
          )

          // Write out errant lines to separate BQ location
          val errorsTableName = s"${outputDataset}_errors"
          Transform.persist[FailedParse](
            invalidTableRows,
            outputProject,
            outputDataset,
            errorsTableName,
            shard,
            WRITE_APPEND,
            CREATE_IF_NEEDED
          )
        }
      }

    result.waitUntilDone()
  }
}
