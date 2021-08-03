package cityblock.member.attribution
import cityblock.member.attribution.runners.GenericRunner
import cityblock.member.service.utilities.MultiPartnerIds
import cityblock.utilities.Environment
import com.spotify.scio.ScioContext
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions

object AttributionRunner {

  /***
   *
   * @param cmdLineArgs: This requires the following arguments to be passed in, in order to run:
   * @param bigQueryProject: The Bigquery project where the silver member data resides.
   * @param bigQueryDataset: The Bigquery dataset where the silver member data resides.
   * @param cohortId: (optional) The cohort associated with the members being attributed.
   * @param env: The environment to use for secret management. This can either be "prod" or "staging".
   * @param deployTo: The partner/environment combo we are running attribution for.
   *
   * Sample run:
   *   sbt "runMain cityblock.member.attribution.AttributionRunner
   *     --deployTo=tuftsstaging
   *     --bigQueryProject=tufts-data
   *     --bigQueryDataset=silver_claims
   *     --cohortId=22
   *     --environment=staging
   *     --tempLocation=gs://internal-tmp-cbh/temp"
   */
  def main(cmdLineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[DataflowPipelineOptions](cmdLineArgs)
    val deployTo = args.required("deployTo")

    val bigQueryProject: String = args.required("bigQueryProject")
    val bigQueryDataset: String = args.required("bigQueryDataset")
    val cohortId: Option[Int] = args.optional("cohortId").map(_.toInt)

    val sc = ScioContext(opts)
    val env = Environment(cmdLineArgs)
    val multiPartnerIds = MultiPartnerIds.getByProject(deployTo)

    // TODO: Split by partner and environment instead
    val runner = GenericRunner(deployTo)
    val bigQuerySrc = runner.query(bigQueryProject, bigQueryDataset)
    runner.deployOnScio(bigQuerySrc, cohortId, env, multiPartnerIds)(sc)
  }
}
