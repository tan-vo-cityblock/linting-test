package cityblock.transforms.carefirst

import java.util.concurrent.TimeUnit

import cityblock.models.CarefirstSilverClaims.SilverProvider
import cityblock.models.gold.NewProvider.Provider
import cityblock.transforms.Transform
import cityblock.transforms.carefirst.gold.ProviderTransformer
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

object PolishCarefirstFullRefreshClaims {
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
    val destinationDataset = args.getOrElse("destinationDataset", "gold_claims")

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
      val results: List[ScioResult] = polishCarefirstRefreshedClaims(sourceProject,
                                                                     sourceDataset,
                                                                     destinationProject,
                                                                     destinationDataset,
                                                                     argv,
                                                                     shard)
      results.foreach(_.waitUntilDone())
    }
  }

  def polishCarefirstRefreshedClaims(
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

    val (providerResult, _) = ScioUtils.runJob("polish-carefirst-transform-gold-provider", args) {
      sc =>
        val provider = ProviderTransformer(fetch[SilverProvider](sc, "provider"), shard)
          .transform()(sourceProject)

        persist[Provider]("Provider", provider)
    }

    List(providerResult)
  }
}
