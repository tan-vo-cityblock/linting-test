package cityblock.utilities

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import javax.xml.bind.DatatypeConverter

object ResultSigner {
  // TODO: Change when we start hitting production
  private val sharedSecret = "BZxrKNCCVudtyAyHuowxQTWd"

  def signResult(result: String): String = {
    val secret = new SecretKeySpec(sharedSecret.getBytes, "SHA-256")
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(secret)
    val hashString: Array[Byte] = mac.doFinal(result.getBytes)
    DatatypeConverter.printHexBinary(hashString).toLowerCase
  }
}
