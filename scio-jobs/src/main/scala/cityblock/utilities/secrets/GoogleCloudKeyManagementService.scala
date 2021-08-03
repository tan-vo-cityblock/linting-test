/*
Util for decrypting secrets from Google Key Management Service.
Note: use of Google KMS has been rejected in favor of Google Secret Manager,
however, member service (as of 2021-05-25) still uses KMS in production, but has migrated to GSM for staging.
This is an artifact of a deprioritized migration project.
 */
package cityblock.utilities.secrets

import com.google.cloud.kms.v1.{CryptoKeyName, KeyManagementServiceClient}
import com.google.cloud.storage.{BlobId, StorageOptions}
import com.google.protobuf.ByteString

trait KMSSecretsSeer extends SecretsSeer[KMSSecret] {
  def decrypt(secret: KMSSecret): String
  def see(secret: KMSSecret): String
}

object KMSSecretsSeer extends KMSSecretsSeer {
  override def decrypt(secret: KMSSecret): String = {
    val filePath = s"${secret.keyName}/${secret.fileName}"

    // First, fetch the encrypted data from GCS
    val storage = StorageOptions.getDefaultInstance.getService
    val blob = storage.get(BlobId.of(secret.gcsBucket, filePath))
    val encrypted = blob.getContent()

    // Next, decrypt the data and return it as a string
    val kmsClient = KeyManagementServiceClient.create
    try {
      val response = kmsClient.decrypt(
        CryptoKeyName.of(secret.keyProject, secret.keyLocation, secret.keyRing, secret.keyName),
        ByteString.copyFrom(encrypted)
      )

      new String(response.getPlaintext.toByteArray)
    } finally {
      if (kmsClient != null) kmsClient.close()
    }
  }

  override def see(secret: KMSSecret): String = decrypt(secret)
}
