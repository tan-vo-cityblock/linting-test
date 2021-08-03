package cityblock.transforms.carefirst

import cityblock.models.gold.Claims.ClaimLineStatus
import cityblock.transforms.Transform
import cityblock.utilities.PartnerConfiguration

package object gold {
  private[gold] val partner: String = PartnerConfiguration.carefirst.indexName

  trait CarefirstLineOrdering[T] {
    protected def lineNumber(t: T): Option[Int]

    object CarefirstLineOrdering extends Ordering[T] {

      /**
       * Compare `x` and `y` by [[lineNumber]], but treat [[None]] as the max value.
       */
      override def compare(x: T, y: T): Int =
        Transform.ClaimLineOrdering.compare(lineNumber(x), lineNumber(y))
    }
  }

  def claimLineStatus(status: String): String =
    status match {
      case "P" => ClaimLineStatus.Paid.toString
      case "D" => ClaimLineStatus.Denied.toString
      case _   => ClaimLineStatus.Unknown.toString
    }
}
