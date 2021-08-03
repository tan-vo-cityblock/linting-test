package cityblock.transforms.carefirst

import java.util.concurrent.TimeUnit

import cityblock.models.CarefirstSilverClaims._
import cityblock.models.gold.Claims.LabResult
import cityblock.models.gold.PharmacyClaim.Pharmacy
import cityblock.transforms.Transform
import cityblock.transforms.carefirst.gold._
import cityblock.utilities.{Environment, PartnerConfiguration, ScioUtils}
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ScioContext, ScioResult}
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object PolishCarefirstWeeklyClaims {
  implicit private val waitDuration: Duration = Duration(1, TimeUnit.HOURS)

  def main(argv: Array[String]): Unit = {
    val (sc, args) = ScioContext.parseArguments[GcpOptions](argv)
    implicit val environment: Environment = Environment(argv)

    val project = sc.getProject

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val shard = LocalDate.parse(args.required("deliveryDate"), dtf)

    val sourceProject =
      args.getOrElse("sourceProject", PartnerConfiguration.carefirst.productionProject)
    val sourceDataset = args.getOrElse("sourceDataset", "silver_claims")

    val destinationProject =
      args.getOrElse("destinationProject", PartnerConfiguration.carefirst.productionProject)
    val destinationDataset = args.getOrElse("destinationDataset", "gold_claims_incremental")

    // this flag is primarily useful when testing airflow DAGs
    val checkArgsAndExit = args.boolean("checkArgsAndExit", default = false)

    if (project != "cityblock-data") {
      throw new Error("""
                        |Running PolishCarefirstData in a project other than "cityblock-data". This
                        |would likely fail due to permissions errors. Please rerun with
                        |--project=cityblock-data.
        """.stripMargin)
    }

    if (!checkArgsAndExit) {
      val results: List[ScioResult] = polishCarefirstWeeklyClaims(sourceProject,
                                                                  sourceDataset,
                                                                  destinationProject,
                                                                  destinationDataset,
                                                                  argv,
                                                                  shard)
      results.foreach(_.waitUntilDone())
    }
  }

  def polishCarefirstWeeklyClaims(
    sourceProject: String,
    sourceDataset: String,
    destinationProject: String,
    destinationDataset: String,
    args: Array[String],
    shard: LocalDate)(implicit environment: Environment): List[ScioResult] = {
    def fetch[T <: HasAnnotation: ClassTag: TypeTag: Coder](sc: ScioContext,
                                                            table: String): SCollection[T] =
      Transform.fetchFromBigQuery[T](sc, sourceProject, sourceDataset, table, shard)

    def persist[T <: HasAnnotation: ClassTag: TypeTag: Coder](
      table: String,
      rows: SCollection[T]): Future[Tap[T]] =
      Transform.persist(rows,
                        destinationProject,
                        destinationDataset,
                        table,
                        shard,
                        WRITE_TRUNCATE,
                        CREATE_IF_NEEDED)

    val (_, indices) =
      ScioUtils.runJob("polish-carefirst-setup", args) { sc =>
        val xs =
          Indices.apply(sourceProject,
                        fetch[SilverDiagnosisAssociation](sc, "diagnosis_association"),
                        fetch[SilverProcedureAssociation](sc, "procedure_association"))

        xs.futures
      }

    val (labsResult, _) = ScioUtils.runJob("polish-carefirst-transform-gold-lab-results", args) {
      sc =>
        val lab = LabResultTransformer(fetch[SilverLabResult](sc, "lab_result"))
          .transform()(sourceProject)

        persist[LabResult]("LabResult", lab)
    }

    val (professionalResult, _) = ScioUtils.runJob(s"polish-carefirst-professional", args) { sc =>
      val professional = ProfessionalTransformer(
        fetch[SilverProfessional](sc, "professional"),
        ScioUtils.waitAndOpen(indices.diagnosis, sc),
        Transform.fetchView[SilverProvider](sc, sourceProject, sourceDataset, "provider")
      ).transform()(sourceProject)

      persist("Professional", professional)
    }

    val (facilityResult, _) = ScioUtils.runJob(s"polish-carefirst-facility", args) { sc =>
      val facility = FacilityTransformer(
        fetch[SilverFacility](sc, "facility"),
        ScioUtils.waitAndOpen(indices.diagnosis, sc),
        ScioUtils.waitAndOpen(indices.procedure, sc)
      ).transform()(sourceProject)

      persist("Facility", facility)
    }

    val (pharmacyResult, _) = ScioUtils.runJob("polish-carefirst-transform-gold-pharmacy", args) {
      sc =>
        val pharmacy = PharmacyTransformer(fetch[SilverPharmacy](sc, "pharmacy"))
          .transform()(sourceProject)

        persist[Pharmacy]("Pharmacy", pharmacy)
    }

    List(professionalResult, labsResult, pharmacyResult, facilityResult)
  }
}
