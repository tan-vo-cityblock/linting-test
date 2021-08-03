package cityblock.member.attribution.runners
import cityblock.member.service.utilities.MultiPartnerIds
import cityblock.utilities.Environment
import com.spotify.scio.{ScioContext, ScioResult}

trait GenericRunner {
  def query(
    bigQueryProject: String,
    bigQueryDataset: String
  ): String

  def deployOnScio(
    bigQuerySrc: String,
    cohortId: Option[Int],
    env: Environment,
    multiPartnerIds: MultiPartnerIds
  ): ScioContext => ScioResult
}

object GenericRunner {
  def apply(partner: String): GenericRunner =
    partner match {
      case "tuftsproduction" | "tuftsstaging"                 => new TuftsRunner()
      case "carefirstproduction" | "carefirststaging"         => new CareFirstRunner()
      case "emblemvirtualproduction" | "emblemvirtualstaging" => new EmblemVirtualRunner()
      case "cardinalproduction" | "cardinalstaging"           => new CardinalRunner()
      case "healthyblueproduction" | "healthybluestaging"     => new HealthyBlueRunner()
    }
}
