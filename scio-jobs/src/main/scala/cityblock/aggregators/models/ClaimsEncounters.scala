package cityblock.aggregators.models

import cityblock.utilities.Environment
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.{Compression, FileIO, TextIO}
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, PaneInfo}
import org.apache.beam.sdk.transforms.{Contextful, SerializableFunction}

import scala.concurrent.Future

object ClaimsEncounters {
  val gcsName = "claims-encounters.v2"

  @BigQueryType.toTable
  case class ClaimsEncounterAddress(
    street1: Option[String],
    street2: Option[String],
    city: Option[String],
    state: Option[String],
    zip: Option[String]
  )

  @BigQueryType.toTable
  case class ClaimsEncounter(
    provider: Option[String],
    summary: Option[String],
    date: Option[String],
    address: ClaimsEncounterAddress,
    placeOfService: Option[String],
    typeOfService: Option[String],
    diagnosis: Option[String],
    description: Option[String]
  )

  case class PatientClaimsEncounters(
    patientId: String,
    encounters: List[ClaimsEncounter]
  )

  object PatientClaimsEncounters {
    private[aggregators] def gcsCustomIOId(implicit environment: Environment): String =
      s"Save $gcsName to GCS (${environment.patientDataBucket})"

    def saveToGCS(aggregated: SCollection[PatientClaimsEncounters])(
      implicit environment: Environment): Future[Tap[PatientClaimsEncounters]] = {
      val bucketName = environment.patientDataBucket

      val transform = FileIO
        .writeDynamic[String, PatientClaimsEncounters]()
        .by(namingFn)
        .via(
          Contextful.fn[PatientClaimsEncounters, String]((x: PatientClaimsEncounters) => {
            import io.circe.generic.auto._
            import io.circe.syntax._
            x.encounters.asJson.noSpaces
          }),
          TextIO.sink()
        )
        .to(bucketName)
        .withDestinationCoder(StringUtf8Coder.of())
        .withNaming(id => new Namer(id, gcsName))
        .withNumShards(1)

      aggregated.saveAsCustomOutput(gcsCustomIOId, transform)
    }

    private def namingFn =
      new SerializableFunction[PatientClaimsEncounters, String] {
        override def apply(row: PatientClaimsEncounters): String =
          row.patientId
      }

    private class Namer(id: String, aggregationType: String) extends FileIO.Write.FileNaming {
      override def getFilename(window: BoundedWindow,
                               pane: PaneInfo,
                               numShards: Int,
                               shardIndex: Int,
                               compression: Compression): String =
        s"$id/aggregated_data/$aggregationType.json"
    }
  }
}
