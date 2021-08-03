package cityblock.transforms.tufts

import cityblock.models.TuftsSilverClaims.SilverMember
import cityblock.transforms.Transform
import cityblock.transforms.tufts.gold.MemberTransformer
import cityblock.utilities.{Environment, PartnerConfiguration, ScioUtils}
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ScioContext, ScioResult}
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag

object PolishDailyTuftsData {
  def main(argv: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[GcpOptions](argv)
    implicit val environment: Environment = Environment(argv)

    val project = opts.getProject

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val shard = LocalDate.parse(args.required("deliveryDate"), dtf)

    val sourceProject =
      args.getOrElse("sourceProject", PartnerConfiguration.tufts.productionProject)
    val sourceDataset = args.getOrElse(
      "sourceDataset",
      "silver_claims"
    )
    val destinationProject =
      args.getOrElse("destinationProject", PartnerConfiguration.tufts.productionProject)
    val destinationDataset = args.getOrElse("destinationDataset", "gold_claims")

    if (project != "cityblock-data") {
      throw new Error("""
          |Running PolishTuftsData in a project other than "cityblock-data". This
          |would likely fail due to permissions errors. Please rerun with
          |--project=cityblock-data.
        """.stripMargin)
    }

    val results =
      polishTuftsData(
        argv,
        sourceDataset,
        sourceProject,
        destinationProject,
        destinationDataset,
        shard
      )

    results.map(_.waitUntilDone())
  }

  def polishTuftsData(
    args: Array[String],
    sourceDataset: String,
    sourceProject: String,
    destinationProject: String,
    destinationDataset: String,
    shard: LocalDate
  )(
    implicit environment: Environment
  ): List[ScioResult] = {
    val writeDisposition = WRITE_TRUNCATE
    val createDisposition = CREATE_IF_NEEDED

    def fetch[T <: HasAnnotation: TypeTag: ClassTag: Coder](sc: ScioContext,
                                                            table: String): SCollection[T] =
      Transform
        .fetchFromBigQuery[T](sc, sourceProject, sourceDataset, table, shard)

    val (memberResult, _) = ScioUtils.runJob("polish-tufts-transform-gold-member-daily", args) {
      sc =>
        val member = MemberTransformer(
          fetch[SilverMember](sc, "Member_Daily")
        ).transform()(sourceProject)

        Transform.persist(
          member,
          destinationProject,
          destinationDataset,
          "Member_Daily",
          shard,
          writeDisposition,
          createDisposition
        )
    }

    List(memberResult)
  }
}
