package cityblock.aggregators.jobs

import cityblock.aggregators.models.PatientEligibilities._
import io.circe.syntax._
import com.spotify.scio.bigquery._
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, PaneInfo}
import org.apache.beam.sdk.transforms.{Contextful, SerializableFunction}
import org.apache.beam.sdk.io.{Compression, FileIO, TextIO}

object PushPatientEligibilities {
  private[jobs] val eligibilityOutputName = "Save Patient Eligibilities to GCS"

  def main(argv: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[DataflowPipelineOptions](argv)
    val sc = ScioContext(opts)

    val sourceProject = args.required("sourceProject")
    val sourceDataset = args.required("sourceDataset")
    val sourceTable = args.required("sourceTable")
    val sourceLocation = s"$sourceProject.$sourceDataset.$sourceTable"
    val destinationBucket = args.required("destinationBucket")
    pushPatientEligibilities(sc, sourceLocation, destinationBucket)

    sc.close().waitUntilDone()
  }

  def pushPatientEligibilities(
    sc: ScioContext,
    sourceLocation: String,
    destinationBucket: String
  ): Unit = {
    val patientEligibilities: SCollection[PatientEligibilities] = sc
      .withName("Load eligibilities")
      .typedBigQuery[PatientEligibilities](eligibilitiesQuery(sourceLocation))

    saveToGCS(patientEligibilities, destinationBucket)
  }

  private[jobs] def eligibilitiesQuery(sourceLocation: String): String =
    s"SELECT * FROM `$sourceLocation`"

  def saveToGCS(
    patientEligibilities: SCollection[PatientEligibilities],
    destinationBucket: String
  ): Unit = {
    val transform: FileIO.Write[String, PatientEligibilities] = FileIO
      .writeDynamic[String, PatientEligibilities]()
      .by(patientEligibilitiesNamingFn)
      .via(
        Contextful.fn[PatientEligibilities, String](
          (_: PatientEligibilities).asJson.noSpaces
        ),
        TextIO.sink()
      )
      .to(destinationBucket)
      .withDestinationCoder(StringUtf8Coder.of)
      .withNaming(new EligibilityNamer(_, "eligibilities"))
      .withNumShards(1)

    patientEligibilities
      .saveAsCustomOutput(eligibilityOutputName, transform)
  }

  private[jobs] def patientEligibilitiesNamingFn =
    new SerializableFunction[PatientEligibilities, String] {
      override def apply(eligibilities: PatientEligibilities): String =
        eligibilities.id.getOrElse("NO_ID")
    }

  private class EligibilityNamer(id: String, aggregationType: String)
      extends FileIO.Write.FileNaming {
    override def getFilename(
      window: BoundedWindow,
      pane: PaneInfo,
      numShards: Int,
      shardIndex: Int,
      compression: Compression
    ): String =
      s"$id/aggregated_data/$aggregationType.json"
  }
}
