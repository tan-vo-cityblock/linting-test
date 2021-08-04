package cityblock.transforms.combine

import cityblock.models.gold.Claims.LabResult
import cityblock.models.gold.FacilityClaim.Facility
import cityblock.models.gold.PharmacyClaim.Pharmacy
import cityblock.models.gold.ProfessionalClaim.Professional
import cityblock.transforms.Transform
import cityblock.utilities.{Environment, ScioUtils}
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object CombineReplaceData {
  def main(argv: Array[String]): Unit = {
    val (_, args) = ScioContext.parseArguments[GcpOptions](argv)
    implicit val environment: Environment = Environment(argv)

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)

    val oldShard = args.required("oldDeliveryDate")
    val newShard = args.required("newDeliveryDate")
    val newDate = LocalDate.parse(newShard, dtf)
    val sourceProject: String = args.required("sourceProject")
    val newSourceDataset: String = args.getOrElse("newSourceDataset", "gold_claims_incremental")
    val oldSourceDataset: String = args.getOrElse("oldSourceDataset", "gold_claims")
    val destinationProject: String = args.getOrElse("destinationProject", sourceProject)
    val destinationDataset: String = args.getOrElse("destinationDataset", "gold_claims")

    val tablesToCombine: List[String] = args.list("tablesToCombine")

    def persist[T <: HasAnnotation: ClassTag: TypeTag: Coder](
      table: String,
      rows: SCollection[T]
    ): Future[Tap[T]] =
      Transform.persist(
        rows,
        destinationProject,
        destinationDataset,
        table,
        newDate,
        WRITE_TRUNCATE,
        CREATE_IF_NEEDED
      )

    def fetch[T <: HasAnnotation: ClassTag: TypeTag: Coder](
      sc: ScioContext,
      sourceDataset: String,
      table: String,
      shard: LocalDate
    ): SCollection[T] =
      Transform.fetchFromBigQuery[T](sc, sourceProject, sourceDataset, table, shard)

    tablesToCombine.map {
      case "Professional" =>
        ScioUtils.runJob(s"combine-$sourceProject-professional", argv) { sc =>
          val oldProf: SCollection[Professional] =
            Combine.fetchProfessionalClaimsAndFixAmounts(sc,
                                                         oldSourceDataset,
                                                         oldShard,
                                                         sourceProject)
          val newProf: SCollection[Professional] =
            Combine.fetchProfessionalClaimsAndFixAmounts(sc,
                                                         newSourceDataset,
                                                         newShard,
                                                         sourceProject)
          val combinedData: SCollection[Professional] =
            combineData[Professional](oldProf, newProf, (p: Professional) => p.claimId)
          persist[Professional]("Professional", combinedData)
        }
      case "Facility" =>
        ScioUtils.runJob(s"combine-$sourceProject-facility", argv) { sc =>
          val oldFacility: SCollection[Facility] =
            Combine.fetchFacilityClaimsAndFixAmounts(sc, oldSourceDataset, oldShard, sourceProject)
          val newFacility: SCollection[Facility] =
            Combine.fetchFacilityClaimsAndFixAmounts(sc, newSourceDataset, newShard, sourceProject)
          val combinedData: SCollection[Facility] =
            combineData[Facility](oldFacility, newFacility, (f: Facility) => f.claimId)
          persist[Facility]("Facility", combinedData)
        }
      case "Pharmacy" =>
        ScioUtils.runJob(s"combine-$sourceProject-pharmacy", argv) { sc =>
          val oldPharmacy: SCollection[Pharmacy] =
            Combine.fetchPharmacyClaimsAndFixAmounts(sc, oldSourceDataset, oldShard, sourceProject)
          val newPharmacy: SCollection[Pharmacy] =
            Combine.fetchPharmacyClaimsAndFixAmounts(sc, newSourceDataset, newShard, sourceProject)
          val combinedData: SCollection[Pharmacy] =
            combineData[Pharmacy](oldPharmacy, newPharmacy, (p: Pharmacy) => p.identifier.id)
          persist[Pharmacy]("Pharmacy", combinedData)
        }
      case "LabResult" =>
        ScioUtils.runJob(s"combine-$sourceProject-labresult", argv) { sc =>
          val oldLabResult: SCollection[LabResult] = fetch[LabResult](
            sc,
            oldSourceDataset,
            "LabResult",
            LocalDate.parse(oldShard, dtf)
          )
          val newLabResult = fetch[LabResult](sc, newSourceDataset, "LabResult", newDate)
          val combinedData =
            combineData[LabResult](oldLabResult, newLabResult, (l: LabResult) => l.identifier.id)
          persist[LabResult]("LabResult", combinedData)
        }
    }
  }

  def combineData[T <: HasAnnotation: ClassTag: TypeTag: Coder](
    oldData: SCollection[T],
    newData: SCollection[T],
    keyByFunction: T => String
  ): SCollection[T] = {
    val joinedData = oldData.keyBy(keyByFunction).fullOuterJoin(newData.keyBy(keyByFunction))
    joinedData.values.map(data => if (data._2.isDefined) data._2.get else data._1.get)
  }
}
