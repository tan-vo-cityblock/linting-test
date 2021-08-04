package cityblock.transforms.connecticare

import java.util.concurrent.TimeUnit

import cityblock.models.ConnecticareSilverClaims._
import cityblock.models.gold.Claims.Member
import cityblock.models.gold.NewMember
import cityblock.transforms.Transform
import cityblock.transforms.connecticare.gold._
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

object PolishCCIFacetsWeeklyData {
  implicit val waitDuration: Duration = Duration(1, TimeUnit.HOURS)

  def main(argv: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[GcpOptions](argv)
    implicit val environment: Environment = Environment(argv)

    val project = opts.getProject

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val shard = LocalDate.parse(args.required("deliveryDate"), dtf)

    val sourceProject =
      args.getOrElse("sourceProject", PartnerConfiguration.connecticare.productionProject)
    val sourceDataset =
      args.getOrElse("sourceDataset", "silver_claims_facets")
    val destinationProject =
      args.getOrElse("destinationProject", PartnerConfiguration.connecticare.productionProject)
    val destinationDataset =
      args.getOrElse("destinationDataset", "gold_claims_facets")

    // this flag is primarily useful when testing airflow DAGs
    val checkArgsAndExit = args.boolean("checkArgsAndExit", default = false)

    if (project != "cityblock-data") {
      throw new Error(
        """
                        |Running PolishCCIFacetsWeeklyData in a project other than "cityblock-data". This
                        |would likely fail due to permissions errors. Please rerun with
                        |--project=cityblock-data.
        """.stripMargin)
    }

    if (!checkArgsAndExit) {
      val results: List[ScioResult] = polishCCIFacetsWeeklyData(
        argv,
        sourceDataset,
        sourceProject,
        destinationProject,
        destinationDataset,
        shard
      )

      results.foreach(_.waitUntilDone())
    }
  }

  def polishCCIFacetsWeeklyData(
    args: Array[String],
    sourceDataset: String,
    sourceProject: String,
    destinationProject: String,
    destinationDataset: String,
    shard: LocalDate
  )(implicit environment: Environment): List[ScioResult] = {
    def fetch[T <: HasAnnotation: TypeTag: ClassTag: Coder](
      sc: ScioContext,
      table: String
    ): SCollection[T] =
      Transform
        .fetchFromBigQuery[T](sc, sourceProject, sourceDataset, table, shard)

    def persist[T <: HasAnnotation: TypeTag: ClassTag: Coder](
      destinationTable: String,
      goldData: SCollection[T]
    ): Future[Tap[T]] =
      Transform.persist(
        goldData,
        destinationProject,
        destinationDataset,
        destinationTable,
        shard,
        WRITE_TRUNCATE,
        CREATE_IF_NEEDED
      )

    /* We're emitting member data in both the v1 (gold.Claims.Member) and v2 (gold.NewMember.Member) schemas because
     * both have consumers.
     *
     * In terms of programmatic consumers, the master member table uses the v2 schema, and other parts of dbt use the v1
     * schema. PublishMemberAttributionData (run via the load_monthly_data_cci_v1 Airflow DAG) also uses the v1 schema.
     *
     * Manual consumers include members of the Data Analytics and Data Science teams.
     * */
    val (memberResult, _): (ScioResult, (Future[Tap[Member]], Future[Tap[NewMember.Member]])) =
      ScioUtils.runJob("polish-facets-gold-member", args) { sc =>
        val silverMember = fetch[FacetsMember](sc, "Member_Facets_med")
        val silverBenefits = fetch[HealthCoverageBenefits](sc, "Health_Coverage_Benefit_med")
        val silverPCP = fetch[AttributedPCP](sc, "Attributed_PCP_med")
        val silverProvider: SCollection[FacetsProvider] =
          Transform.fetchView[FacetsProvider](sc, sourceProject, sourceDataset, "FacetsProvider")

        val memberFacets: SCollection[Member] = MemberFacetsTransformer(
          silverMember,
          silverBenefits,
          silverPCP,
          silverProvider
        ).transform()(sourceProject)

        val memberV2Facets: SCollection[NewMember.Member] = NewMemberFacetsTransformer.transform(
          sourceProject,
          sourceDataset,
          "Member_Facets_med",
          shard,
          silverMember,
          silverBenefits,
          silverPCP
        )

        val memberFuture = persist[Member]("Member", memberFacets)
        val memberV2Future = persist[NewMember.Member]("MemberV2", memberV2Facets)

        // We aren't using this return right now, but it's included to make it very obvious that this function
        // produces two futures
        (memberFuture, memberV2Future)
      }

    List(memberResult)
  }
}
