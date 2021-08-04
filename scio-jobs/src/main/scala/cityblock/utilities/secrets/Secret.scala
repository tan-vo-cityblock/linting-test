package cityblock.utilities.secrets

trait CityblockSecret
trait SecretsSeer[S <: CityblockSecret] {
  def see(secret: S): String
}

case class KMSSecret(
  gcsBucket: String,
  fileName: String,
  keyProject: String,
  keyLocation: String,
  keyRing: String,
  keyName: String
) extends CityblockSecret

case class GSMSecret(
  projectId: String,
  secretId: String,
  versionId: String = "latest"
) extends CityblockSecret
