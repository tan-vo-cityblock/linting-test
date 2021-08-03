package cityblock.transforms.emblem

import cityblock.models.EmblemSilverClaims._
import cityblock.models.gold.Claims._
import cityblock.transforms.Transform
import cityblock.transforms.Transform.{Key, UUIDComparable}
import cityblock.utilities.PartnerConfiguration
import org.joda.time.LocalDate
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import purecsv.safe.converter.StringConverter

import scala.util.Try

package object gold {
  private[gold] val partner: String = PartnerConfiguration.emblem.indexName
  private[gold] val FirstSequenceNumber: String = "01"

  type IndexedProvider = (ProviderKey, Provider)
  type IndexedProviderOption = (Option[ProviderKey], (SilverProvider, Provider))

  sealed case class ClaimKey(claimId: String) extends Key
  object ClaimKey {
    def apply(professional: SilverProfessionalClaimCohort): ClaimKey =
      ClaimKey(professional.claim.CLAIM_ID)
    def apply(facility: SilverFacilityClaimCohort): ClaimKey =
      ClaimKey(facility.claim.CLAIM_ID)
    def apply(diagnosis: SilverDiagnosisAssociation): ClaimKey =
      ClaimKey(diagnosis.diagnosis.CLAIMNUMBER)
    def apply(diagnosis: SilverDiagnosisAssociationCohort): ClaimKey =
      ClaimKey(diagnosis.diagnosis.CLAIMNUMBER)
    def apply(procedure: SilverProcedureAssociation): ClaimKey =
      ClaimKey(procedure.procedure.CLAIMNUMBER)
  }

  sealed case class PriorAuthorizationKey(priorAuthId: String) extends Key with UUIDComparable {
    override protected val elements: List[String] = List(partner, priorAuthId)
  }

  /**
   * Creates an [[Ordering]] for a silver claim line [[T]] by `SV_LINE`. Places claim lines
   * without a line number last in an ascending list.
   * @tparam T silver claim type
   */
  trait EmblemLineOrdering[T] {
    protected def svLine(t: T): Option[Int]

    object EmblemLineOrdering extends Ordering[T] {

      /**
       * Compare `x` and `y` by [[svLine]], but treat [[None]] as the max value.
       */
      override def compare(x: T, y: T): Int =
        Transform.ClaimLineOrdering.compare(svLine(x), svLine(y))
    }
  }

  private[gold] def claimLineStatus(SV_STAT: String): Option[String] = {
    val status = SV_STAT match {
      case "P" => Some(ClaimLineStatus.Paid)
      case "D" => Some(ClaimLineStatus.Denied)
      case "O" => Some(ClaimLineStatus.Open)
      case "V" => Some(ClaimLineStatus.Voided)
      case "E" => Some(ClaimLineStatus.Encounter)
      case _   => None
    }
    status.map(_.toString)
  }

  private[emblem] def cobFlag(COBINDICATOR: Option[String]): Option[Boolean] =
    COBINDICATOR.map {
      case "Y" | "A" => true
      case _         => false
    }

  private[emblem] def capitatedFlag(FFSCAPIND: Option[String]): Option[Boolean] =
    FFSCAPIND.flatMap {
      case "F" => Some(false)
      case "C" => Some(true)
      case _   => None
    }

  /**
   * Checks whether the Emblem claim's `SV_LINE` value indicates the first claim line.
   */
  private[gold] def isFirstLine(SV_LINE: Option[Int]): Boolean =
    SV_LINE.contains(1)

  private[gold] object YearMonthDateParser {

    def parse(s: String): Option[LocalDate] =
      localDateStringConverter.tryFrom(s).toOption

    private val EmblemTimestampPattern =
      "\\A\\d{2}\\w{3}\\d{4}:\\d{2}:\\d{2}:\\d{2}\\z".r
    private val EmblemTimestampFormatter =
      DateTimeFormat.forPattern("ddMMMyyyy:HH:mm:ss z")
    private val EmblemYearMonthPattern = "\\A201\\d[0-1]\\d\\z".r
    private val EmblemYearMonthFormatter = DateTimeFormat.forPattern("yyyyMM z")
    private val EmblemTextMonthPattern =
      "^(([0-9])|([0-2][0-9])|([3][0-1]))\\-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\\-(\\d{2})$".r
    private val EmblemTextMonthFormatter =
      DateTimeFormat.forPattern("dd-MMM-yy")

    val localDateStringConverter = new StringConverter[LocalDate] {
      override def tryFrom(column: String): Try[LocalDate] =
        column match {
          case EmblemTimestampPattern() =>
            Try(LocalDate.parse(column ++ " UTC", EmblemTimestampFormatter))
          case EmblemYearMonthPattern() =>
            Try(LocalDate.parse(column ++ " UTC", EmblemYearMonthFormatter))
          case EmblemTextMonthPattern(_*) =>
            Try(LocalDate.parse(column, EmblemTextMonthFormatter))
          case _ => Try(LocalDate.parse(column))
        }

      override def to(dateTime: LocalDate): String =
        ISODateTimeFormat.dateTimeNoMillis().print(dateTime)
    }
  }
}
