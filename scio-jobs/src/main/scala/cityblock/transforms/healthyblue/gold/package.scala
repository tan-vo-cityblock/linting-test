package cityblock.transforms.healthyblue

import cityblock.models.gold.Claims.ClaimLineStatus
import cityblock.utilities.{Conversions, PartnerConfiguration}

package object gold {
  private[gold] val partner: String = PartnerConfiguration.healthyblue.indexName

  private[gold] def mkClaimLineStatus(status: Option[String]): String =
    status match {
      case Some("P") => ClaimLineStatus.Paid.toString
      case Some("D") => ClaimLineStatus.Denied.toString
      case _         => ClaimLineStatus.Unknown.toString
    }

  private[gold] def mkLineNumber(lineNumber: Option[String]): Int =
    lineNumber.flatMap(Conversions.safeParse(_, _.toInt)).getOrElse(0)
}
