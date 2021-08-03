package cityblock.utilities.secrets

import com.google.cloud.secretmanager.v1beta1.SecretManagerServiceClient
import com.google.cloud.secrets.v1beta1.SecretVersionName

trait GSMSecretsSeer extends SecretsSeer[GSMSecret] {
  def getData(secret: GSMSecret): String
  def see(see: GSMSecret): String
}

object GSMSecretsSeer extends GSMSecretsSeer {
  override def getData(secret: GSMSecret): String = {
    val GSMSecret(projectId, secretId, versionId) = secret
    val client = SecretManagerServiceClient.create
    try {
      val secretVersionName = SecretVersionName.of(projectId, secretId, versionId)
      val response = client.accessSecretVersion(secretVersionName)
      response.getPayload.getData.toStringUtf8
    } finally {
      if (client != null) client.close()
    }
  }

  override def see(secret: GSMSecret): String = getData(secret)
}
