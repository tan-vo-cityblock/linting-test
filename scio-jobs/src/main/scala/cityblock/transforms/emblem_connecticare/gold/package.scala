package cityblock.transforms.emblem_connecticare

import java.util.UUID

package object gold {
  def mapPayerName(companyCode: Option[String]): String =
    companyCode match {
      case Some("HIP") => "emblem"
      case Some("CCI") => "connecticare"
      case _           => "Unknown"
    }

  def mkProviderId(providerIdField: String): String =
    UUID.nameUUIDFromBytes(providerIdField.getBytes()).toString
}
