package cityblock.scripts

import cityblock.streaming.jobs.aggregations.Encounters.PatientEncounter
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.SCollection
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.{Compression, FileIO, TextIO}
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, PaneInfo}
import org.apache.beam.sdk.transforms.{Contextful, SerializableFunction}
import io.circe.syntax._
import com.spotify.scio.bigquery._
import org.joda.time.Instant

/**
 * LoadPatientEncountersV2ToGCS is only meant to be RUN ONCE.
 * The job uses a query to pull down the current state of PatientEncounters objects from medical.patient_encounters_v2.
 * The job then writes the objects as json files to GCS for ingestion into Commons.
 * Usage:
 * `runMain cityblock.scripts.LoadPatientEncountersV2ToGCS --runner=DirectRunner --project=cityblock-data
 * --v2FullViewName=cityblock-data.dev_medical.latest_patient_encounters
 * --v2Bucket=gs://cityblock-production-patient-data --tempLocation=gs://cbh-kola-awe-scratch/encountersV2Temp`
 */
object LoadPatientEncountersV2ToGCS {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val patientEncountersV2View = args.required("v2FullViewName")
    val patientEncountersV2Bucket = args.required("v2Bucket")
    val queryLimit = args.getOrElse("queryLimit", "")
    val limitStatement =
      if (queryLimit.isEmpty) "" else s"LIMIT $queryLimit"

    val patientEncounters = sc.typedBigQuery[PatientEncounter](s"""
        |SELECT * FROM `$patientEncountersV2View` $limitStatement
        |""".stripMargin)

    val encountersForGCS: SCollection[List[PatientEncounter]] =
      patientEncounters
        .groupBy(
          _.messageId
        )
        .map(_._2.toList)

    savePatientEncounterListToGCS(encountersForGCS, patientEncountersV2Bucket)

    sc.close().waitUntilFinish()
  }

  private def savePatientEncounterListToGCS(
    patientEncounters: SCollection[List[PatientEncounter]],
    patientDataBucket: String
  ): Unit = {
    implicit val instantEncoder: Encoder[Instant] = Encoder.instance(a => a.toString.asJson)
    implicit val patientEncounterEncoder: Encoder[PatientEncounter] =
      deriveEncoder[PatientEncounter]

    val transform = FileIO
      .writeDynamic[String, List[PatientEncounter]]()
      .by(patientEncountersNamingFn())
      .via(
        Contextful.fn[List[PatientEncounter], String](
          (encounters: List[PatientEncounter]) => encounters.asJson.noSpaces
        ),
        TextIO.sink()
      )
      .to(patientDataBucket)
      .withDestinationCoder(StringUtf8Coder.of())
      .withNaming(new DiagnosesNamer(_, "encounters.v2"))
      .withNumShards(1)

    //The .groupBy groups lists of PatientEncounter objects by key patientId or key None if no patientId string exists.
    //The .flatMap call checks that patientId key exists and then uses maxBy to grab the list with a head Encounter
    //that has the highest messageId. Note that each list in the iterable of lists was originally grouped by messageId
    patientEncounters
      .groupBy[Option[String]] {
        case head :: _ => head.patient.patientId
        case _         => None
      }
      .flatMap {
        case (Some(_), patientEncounterListsIterable) =>
          Seq(patientEncounterListsIterable.toList.maxBy {
            case head :: _ => head.messageId
            case _         => "0"
          })
        case _ => Seq()
      }
      .saveAsCustomOutput("Save Patient Encounters to GCS", transform)
  }

  private def patientEncountersNamingFn() =
    new SerializableFunction[List[PatientEncounter], String] {
      override def apply(encounters: List[PatientEncounter]): String =
        encounters match {
          case head :: _ => head.patient.patientId.getOrElse("")
          case _         => "NAMING_FAILED"
        }
    }

  private class DiagnosesNamer(id: String, aggregationType: String)
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
