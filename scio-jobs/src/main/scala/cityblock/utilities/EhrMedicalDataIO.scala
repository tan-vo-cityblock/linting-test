package cityblock.utilities

import cityblock.streaming.jobs.aggregations.Encounters._
import cityblock.streaming.jobs.aggregations.Medications._
import cityblock.streaming.jobs.aggregations.Problems._
import cityblock.streaming.jobs.aggregations.VitalSigns._
import cityblock.streaming.jobs.common.ParsedPatientMedicalDataLists
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.bigquery.{CREATE_IF_NEEDED, WRITE_APPEND}
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import io.circe.Encoder
import io.circe.syntax._
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition
import org.apache.beam.sdk.io.{Compression, FileIO, TextIO}
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, PaneInfo}
import org.apache.beam.sdk.transforms.{Contextful, SerializableFunction}
import org.joda.time.Instant

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

trait EhrMedicalDataIO extends Loggable with Serializable {
  def encountersFileName: String = "encounters.v3"
  def medicationsFileName: String = "medications"
  def vitalsFileName: String = "vital_signs"

  //TODO: Make persisting to BigQuery a utility function since it is done in multiple classes/objects.
  def persistPatientMedicalData[T <: HasAnnotation: TypeTag: ClassTag: Coder](
    data: SCollection[List[T]],
    tableName: String,
    writeDisposition: WriteDisposition = WRITE_APPEND
  )(implicit environment: Environment): Unit =
    data
      .flatten[T]
      .saveAsTypedBigQuery(
        s"${environment.projectName}:$tableName",
        writeDisposition,
        CREATE_IF_NEEDED
      )

  def aggregateAndPersistPatientMedicalData(
    patientMedicalDataLists: ParsedPatientMedicalDataLists,
    medicalDataset: String,
    writeDisposition: WriteDisposition = WRITE_APPEND
  )(implicit environment: Environment): Unit = {
    val problemLists = patientMedicalDataLists.patientProblemLists
    val medicationLists = patientMedicalDataLists.patientMedicationLists
    val encounterLists = patientMedicalDataLists.patientEncounterLists
    val vitalSignLists = patientMedicalDataLists.patientVitalSignLists

    persistPatientMedicalData(
      problemLists,
      s"$medicalDataset.patient_problems",
      writeDisposition
    )
    savePatientProblemListToGCS(problemLists)

    persistPatientMedicalData(
      medicationLists,
      s"$medicalDataset.patient_medications",
      writeDisposition
    )
    savePatientMedicationListToGCS(medicationLists)

    persistPatientMedicalData(
      encounterLists,
      s"$medicalDataset.patient_encounters",
      writeDisposition
    )
    savePatientEncounterListToGCS(encounterLists)

    persistPatientMedicalData(
      vitalSignLists,
      s"$medicalDataset.patient_vital_signs",
      writeDisposition
    )
    savePatientVitalSignListToGCS(vitalSignLists)

    val resultsTable: String = s"$medicalDataset.patient_results"
    for { resultLists <- patientMedicalDataLists.patientResultLists } yield {
      persistPatientMedicalData(
        resultLists,
        resultsTable,
        writeDisposition
      )
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

  def savePatientProblemListToGCS(
    patientProblems: SCollection[List[PatientProblem]]
  )(implicit environment: Environment): Unit = {
    val transform = dynamicDestinations[PatientProblem](
      patientProblemsNamingFn(),
      problems => problems.asJson.noSpaces,
      "diagnoses"
    )

    patientProblems
      .groupBy[Option[String]] {
        case head :: _ => head.patient.patientId
        case _         => None
      }
      .collect {
        case (Some(_), patientProblemLists) =>
          val latestProblemList = patientProblemLists.toList.maxBy {
            case head :: _ => head.messageId
            case _         => "0"
          }

          val filteredProblemList =
            latestProblemList.filter(patientProblem => {
              val code = patientProblem.problem.Code
              val name = patientProblem.problem.Name

              name match {
                case Some(_) => code != ""
                case _       => false
              }
            })

          filteredProblemList
      }
      .saveAsCustomOutput("Save Patient Problems to GCS", transform)
  }

  // TODO make 'save*ToGCS' functions generic
  def savePatientMedicationListToGCS(
    patientMedications: SCollection[List[PatientMedication]]
  )(implicit environment: Environment): Unit = {
    val transform = dynamicDestinations[PatientMedication](
      patientMedicationsNamingFn(),
      medications => medications.asJson.noSpaces,
      medicationsFileName
    )

    patientMedications
      .groupBy[Option[String]] {
        case head :: _ => head.patient.patientId
        case _         => None
      }
      .collect {
        case (Some(_), patientMedicationList) =>
          patientMedicationList.toList.maxBy {
            case head :: _ => head.messageId
            case _         => "0"
          }
      }
      .saveAsCustomOutput("Save Patient Medications to GCS", transform)
  }

  def savePatientEncounterListToGCS(
    patientEncounters: SCollection[List[PatientEncounter]]
  )(implicit environment: Environment): Unit = {
    implicit val instantEncoder: Encoder[Instant] =
      Encoder.instance(a => a.toString.asJson)

    val transform = dynamicDestinations[PatientEncounter](
      patientEncountersNamingFn(),
      encounters => encounters.asJson.noSpaces,
      encountersFileName
    )

    patientEncounters
      .groupBy[Option[String]] {
        case head :: _ => head.patient.patientId
        case _         => None
      }
      .collect {
        case (Some(_), patientEncounterList) =>
          patientEncounterList.toList.maxBy {
            case head :: _ => head.messageId
            case _         => "0"
          }
      }
      .saveAsCustomOutput("Save Patient Encounters to GCS", transform)
  }

  def savePatientVitalSignListToGCS(
    patientVitalSigns: SCollection[List[PatientVitalSign]]
  )(implicit environment: Environment): Unit = {
    val transform = dynamicDestinations[PatientVitalSign](
      patientVitalSignsNamingFn(),
      vitalSigns => vitalSigns.asJson.noSpaces,
      vitalsFileName
    )

    patientVitalSigns
      .groupBy[Option[String]] {
        case head :: _ => head.patient.patientId
        case _         => None
      }
      .collect {
        case (Some(_), patientVitalSignList) =>
          patientVitalSignList.toList.maxBy {
            case head :: _ => head.messageId
            case _         => "0"
          }
      }
      .saveAsCustomOutput("Save Patient Vital Signs to GCS", transform)
  }

  // Suggestion: This should live in a Scio Related Context something like ScioUtils
  private def dynamicDestinations[T](
    namingFn: SerializableFunction[List[T], String],
    contextFn: SerializableFunction[List[T], String],
    aggregationType: String,
    numShards: Int = 1
  )(implicit environment: Environment): FileIO.Write[String, List[T]] =
    FileIO
      .writeDynamic[String, List[T]]()
      .by(namingFn)
      .via(
        Contextful.fn[List[T], String](contextFn),
        TextIO.sink()
      )
      .to(environment.patientDataBucket)
      .withDestinationCoder(StringUtf8Coder.of())
      .withNaming(new DiagnosesNamer(_, aggregationType))
      .withNumShards(numShards)

  // TODO: Make these generic (will need to work around Scio's BQ type provider)
  private def patientProblemsNamingFn() =
    new SerializableFunction[List[PatientProblem], String] {
      override def apply(problems: List[PatientProblem]): String =
        problems match {
          case head :: _ => head.patient.patientId.getOrElse("")
          case _         => "NAMING_FAILED"
        }
    }

  private def patientMedicationsNamingFn() =
    new SerializableFunction[List[PatientMedication], String] {
      override def apply(medications: List[PatientMedication]): String =
        medications match {
          case head :: _ => head.patient.patientId.getOrElse("")
          case _         => "NAMING_FAILED"
        }
    }

  private def patientEncountersNamingFn() =
    new SerializableFunction[List[PatientEncounter], String] {
      override def apply(encounters: List[PatientEncounter]): String =
        encounters match {
          case head :: _ => head.patient.patientId.getOrElse("")
          case _         => "NAMING_FAILED"
        }
    }

  private def patientVitalSignsNamingFn() =
    new SerializableFunction[List[PatientVitalSign], String] {
      override def apply(vitalSigns: List[PatientVitalSign]): String =
        vitalSigns match {
          case head :: _ => head.patient.patientId.getOrElse("")
          case _         => "NAMING_FAILED"
        }
    }
}
