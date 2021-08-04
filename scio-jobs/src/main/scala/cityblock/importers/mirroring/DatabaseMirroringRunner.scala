package cityblock.importers.mirroring

import cityblock.importers.mirroring.config.CsvPostgresTableConfig
import cityblock.utilities.{Environment, Loggable}
import com.google.cloud.storage.StorageOptions
import com.spotify.scio.ScioContext
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

object DatabaseMirroringRunner extends Loggable {
  private val TABLE_GROUP_BATCH_SIZE = 15

  case class DatabaseMirroringConfig(
    outputProject: String,
    outputDataset: String,
    shard: LocalDate
  )

  def main(argv: Array[String]): Unit = {
    val (_, args) = ScioContext.parseArguments[DataflowPipelineOptions](argv)
    implicit val environment: Environment = Environment(argv)

    val inputBucket = args.required("inputBucket")
    val outputProject = args.required("outputProject")
    val outputDataset = args.required("outputDataset")
    val databaseName = args.required("databaseName")
    val tables = args.list("tables")
    val shardDate = args.optional("shardDate")

    logger.info(
      s"""
         | Starting database mirroring job with arguments: {
         |   inputBucket: $inputBucket,
         |   outputProject: $outputProject,
         |   outputDataset: $outputDataset,
         |   databaseName: $databaseName,
         |   tables: $tables,
         |   shardDate: $shardDate
         | }
       """.stripMargin
    )

    val gcsStorageService = StorageOptions.getDefaultInstance.getService
    val dtf = DateTimeFormat.forPattern("YYYY-MM-dd")

    val shard = shardDate
      .filterNot(_.isEmpty)
      .map(LocalDate.parse(_, dtf))
      .getOrElse(LocalDate.now())

    val databaseMirroringConfig = DatabaseMirroringConfig(
      outputProject,
      outputDataset,
      shard
    )

    val batchedTables = tables.grouped(TABLE_GROUP_BATCH_SIZE).toList

    batchedTables.foreach { currentTableBatch: List[String] =>
      val inputConfig = CsvPostgresTableConfig(
        inputBucket,
        databaseName,
        currentTableBatch,
        shard
      )

      environment match {
        case Environment.Prod | Environment.Staging =>
          CsvPostgresTableConfig.verifyAllFilesPresent(
            inputConfig,
            gcsStorageService
          )
        case _ => Unit
      }

      DatabaseMirroring.runMirroringJob(
        argv,
        inputConfig,
        databaseMirroringConfig,
        gcsStorageService
      )
    }
  }
}
