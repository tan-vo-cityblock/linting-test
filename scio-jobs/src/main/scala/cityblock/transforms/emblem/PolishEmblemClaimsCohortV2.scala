package cityblock.transforms.emblem

import java.util.concurrent.TimeUnit

import cityblock.importers.emblem.members.MemberDemographics
import cityblock.models.EmblemSilverClaims._
import cityblock.transforms.Transform
import cityblock.transforms.emblem.gold._
import cityblock.utilities.reference.tables.LineOfBusiness
import cityblock.utilities.{Environment, ScioUtils}
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ContextAndArgs, ScioContext, ScioResult}
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object PolishEmblemClaimsCohortV2 {
  implicit private val waitDuration: Duration = Duration(1, TimeUnit.HOURS)

  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    implicit val environment: Environment = Environment(argv)

    val project = sc.optionsAs[GcpOptions].getProject

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val shard = LocalDate.parse(args.required("deliveryDate"), dtf)

    val sourceProject = args.required("sourceProject")
    val sourceDataset = args.required("sourceDataset")

    val destinationProject = args.required("destinationProject")
    val destinationDataset = args.required("destinationDataset")

    // this flag is primarily useful when testing airflow DAGs
    val checkArgsAndExit = args.boolean("checkArgsAndExit", default = false)

    if (project != "cityblock-data") {
      throw new Error("""
                        |Running PolishEmblemData in a project other than "cityblock-data". This
                        |would likely fail due to permissions errors. Please rerun with
                        |--project=cityblock-data.
        """.stripMargin)
    }

    if (!checkArgsAndExit) {
      val results: List[ScioResult] =
        polishEmblemClaims(
          sourceProject,
          sourceDataset,
          destinationProject,
          destinationDataset,
          argv,
          shard
        )

      results.map(_.waitUntilDone())
    }
  }

  def polishEmblemClaims(
    sourceProject: String,
    sourceDataset: String,
    destinationProject: String,
    destinationDataset: String,
    args: Array[String],
    shard: LocalDate
  )(implicit environment: Environment): List[ScioResult] = {

    def fetch[T <: HasAnnotation: ClassTag: TypeTag: Coder](table: String,
                                                            sc: ScioContext): SCollection[T] =
      Transform.fetchFromBigQuery(sc, sourceProject, sourceDataset, table, shard)

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

    val jobSuffix: String = shard.toString(Transform.ShardNamePattern)

    val (_, indices) =
      ScioUtils.runJob(s"polish-emblem-setup-$jobSuffix", args) { sc =>
        val xs =
          Indices.applyCohort(sourceProject,
                              fetch[SilverDiagnosisAssociationCohort]("diagnosis_associations", sc),
                              fetch[SilverProcedureAssociation]("procedure_associations", sc))

        xs.futures
      }

    val (providerResult, _) =
      ScioUtils.runJob(s"polish-emblem-transform-to-gold-provider-$jobSuffix", args) { sc =>
        val provider =
          ProviderTransformer(fetch[SilverProvider]("providers", sc), shard)
            .transform()(sourceProject)

        persist("Provider", provider)
      }

    val (memberResult, _) = ScioUtils.runJob(s"polish-emblem-member-$jobSuffix", args) { sc =>
      val demographics: SCollection[SilverMemberDemographic] =
        fetch[SilverMemberDemographicCohort]("member_demographics", sc).map { m =>
          SilverMemberDemographic(
            identifier = m.identifier,
            patient = m.patient,
            demographic = MemberDemographics.toGeneralDemographics(m.demographic)
          )
        }

      val keyedLOBMap = LineOfBusiness.fetchAll(sc).keyBy(_.BEN_PKG_ID).asMapSideInput

      val member = MemberTransformer(
        demographics,
        keyedLOBMap,
        fetch[SilverMemberMonth]("member_month", sc)
      ).transform()(sourceProject)

      persist("Member", member)
    }

    val (professionalResult, _) = ScioUtils.runJob(s"polish-emblem-professional-$jobSuffix", args) {
      sc =>
        val keyedLOBMap = LineOfBusiness.fetchAll(sc).keyBy(_.BEN_PKG_ID).asMapSideInput

        val professional = ProfessionalCohortTransformer(
          fetch[SilverProfessionalClaimCohort]("professional", sc),
          keyedLOBMap,
          ScioUtils.waitAndOpen(indices.diagnosis, sc)
        ).transform()(sourceProject)

        persist("Professional", professional)
    }

    val (facilityResult, _) = ScioUtils.runJob(s"polish-emblem-facility-$jobSuffix", args) { sc =>
      val keyedLOBMap = LineOfBusiness.fetchAll(sc).keyBy(_.BEN_PKG_ID).asMapSideInput

      val facility = FacilityCohortTransformer(
        fetch[SilverFacilityClaimCohort]("facility", sc),
        keyedLOBMap,
        ScioUtils.waitAndOpen(indices.diagnosis, sc),
        ScioUtils.waitAndOpen(indices.procedure, sc)
      ).transform()(sourceProject)

      persist("Facility", facility)
    }

    val (pharmacyResult, _) = ScioUtils.runJob(s"polish-emblem-pharmacy-$jobSuffix", args) { sc =>
      val keyedLOBMap = LineOfBusiness.fetchAll(sc).keyBy(_.BEN_PKG_ID).asMapSideInput

      val pharmacy =
        PharmacyCohortTransformer(
          fetch[SilverPharmacyClaimCohort]("pharmacy", sc),
          keyedLOBMap,
          shard
        ).transform()(sourceProject)

      persist("Pharmacy", pharmacy)
    }

    val (labResultResult, _) = ScioUtils.runJob(s"polish-emblem-lab-result-$jobSuffix", args) {
      sc =>
        val labResult = LabResultCohortTransformer(fetch[SilverLabResultCohort]("lab_results", sc))
          .transform()(sourceProject)

        persist("LabResult", labResult)
    }

    List(
      providerResult,
      memberResult,
      professionalResult,
      facilityResult,
      pharmacyResult,
      labResultResult
    )
  }
}
