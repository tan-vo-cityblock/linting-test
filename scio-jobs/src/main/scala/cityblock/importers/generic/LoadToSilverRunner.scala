package cityblock.importers.generic

import cityblock.importers.generic.LoadToSilver._
import cityblock.importers.generic.config.PartnerInputFileConfig
import cityblock.transforms.Transform
import cityblock.utilities.{Environment, Loggable}
import com.google.cloud.storage.Blob.BlobSourceOption
import com.google.cloud.storage.{BlobId, StorageOptions}
import com.spotify.scio.ScioContext
import io.circe.parser._
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

import scala.io.Source

object LoadToSilverRunner extends Loggable {
  // Dataflow has a limit on the size of the graph it can execute, so we split the import into
  // multiple jobs. This controls the number of configs to process per job.
  val MAX_CONFIGS_PER_JOB = 8

  def main(argv: Array[String]): Unit = {
    val (_, args) = ScioContext.parseArguments[DataflowPipelineOptions](argv)
    implicit val environment: Environment = Environment(argv)

    val inputConfigBucket = args.optional("inputConfigBucket")
    val inputConfigPaths = args.list("inputConfigPaths")
    val deliveryDate = args.required("deliveryDate")
    val outputProject = args.required("outputProject")
    val tableShardDate = args.optional("tableShardDate")
    val writeDisposition = args.optional("writeDisposition")

    // `outputDataset` must exist in `outputProject` before running this job
    val outputDataset = args.getOrElse("outputDataset", "silver_claims")

    val maxConfigsPerJob = args.int("maxConfigsPerJob", default = MAX_CONFIGS_PER_JOB)

    // this flag is primarily useful when testing airflow DAGs
    val validateOnly = args.boolean("validateOnly", default = false)

    val allInputConfigs = parseInputConfigs(inputConfigBucket, inputConfigPaths)
    validateInputConfigs(allInputConfigs, deliveryDate)

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val shardString = tableShardDate.getOrElse(deliveryDate)
    val shard = LocalDate.parse(shardString, dtf)

    val partnerName = allInputConfigs.head.partnerName.replaceAll("\\s", "")

    if (!validateOnly) {
      // Each job returns a future for the collection of rows that couldn't be parsed.
      val invalidRowFutures =
        allInputConfigs
          .grouped(maxConfigsPerJob)
          // Note that we have to convert this to an array here to make sure that the individual jobs
          // are kicked off in parallel.
          .toArray
          .zipWithIndex
          .map {
            case (inputConfigs, index) =>
              runLoadToSilverJob(
                argv,
                inputConfigs,
                partnerName,
                deliveryDate,
                outputProject,
                outputDataset,
                shard,
                index,
                writeDisposition
              )
          }

      runWriteErrorsToBigQueryJob(
        argv,
        invalidRowFutures,
        partnerName,
        deliveryDate,
        outputProject,
        outputDataset,
        shard
      )
    }
  }

  private def parseInputConfigs(
    inputConfigBucket: Option[String],
    inputConfigPaths: List[String]
  ): List[PartnerInputFileConfig] =
    inputConfigPaths
      .map { inputConfigPath =>
        logger.info(s"Parsing $inputConfigPath")
        val configAsString = inputConfigBucket match {
          case Some(bucket) => readFileFromGCS(bucket, inputConfigPath)
          case None         => readFileLocal(inputConfigPath)
        }

        parse(configAsString) match {
          case Left(e) => throw e
          case Right(json) =>
            json.as[PartnerInputFileConfig] match {
              case Left(e)       => throw e
              case Right(config) => config
            }
        }
      }

  private def readFileFromGCS(bucket: String, path: String): String = {
    val storage = StorageOptions.getDefaultInstance.getService
    val blob = storage.get(BlobId.of(bucket, path))
    val bytes = blob.getContent(BlobSourceOption.generationMatch())

    new String(bytes)
  }

  private def readFileLocal(path: String): String = {
    val inputConfigFile = Source.fromFile(path)
    val contents = inputConfigFile.getLines.mkString
    inputConfigFile.close

    contents
  }

  private def validateInputConfigs(
    configs: List[PartnerInputFileConfig],
    deliveryDate: String
  ): Unit = {
    // There should be at least one config
    require(configs.nonEmpty, "There must be at least one config")

    // Each config should individually validate
    configs.foreach(_.validate())

    // All configs should be referencing the same partner
    require(
      configs.forall(_.partnerName == configs.head.partnerName),
      "All files should be for the same partner"
    )

    // All files referenced in configs should be present.
    verifyAllFilesPresent(configs, deliveryDate)
  }

  private def verifyAllFilesPresent(
    configs: List[PartnerInputFileConfig],
    deliveryDate: String
  ): Unit =
    // If the file a config is looking for does not exist an error will be thrown.
    configs.foreach(_.getInputFilePath(deliveryDate))
}
