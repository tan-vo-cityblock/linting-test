package cityblock.ehrmodels.redox

import cityblock.utilities.Loggable
import cityblock.utilities.time.DateOrInstant
import io.circe.Decoder
import org.joda.time.{Instant, LocalDate}

import scala.util.control.NonFatal

package object datamodel {
  object DateOrInstantDecoder extends Loggable {
    final lazy val STANDARD_SQL_MIN_INSTANT = new Instant("0001-01-01T00:00:00.000Z").getMillis
    final lazy val STANDARD_SQL_MAX_INSTANT = new Instant("9999-12-31T23:59:59.999Z").getMillis

    private def validateInstant(instant: Instant): Boolean =
      instant.getMillis >= STANDARD_SQL_MIN_INSTANT && instant.getMillis <= STANDARD_SQL_MAX_INSTANT

    private def validDateOrInstant(doi: DateOrInstant): Boolean =
      doi match {
        case DateOrInstant(_, Some(instant), _) => validateInstant(instant)
        case _                                  => true
      }

    private def parseDateOrInstant(raw: Option[String]): Option[DateOrInstant] =
      raw
        .filter(_.nonEmpty)
        .map(rawToDateOrInstant)
        .filter(validDateOrInstant)

    private def rawToDateOrInstant(raw: String): DateOrInstant = {
      def toLocalDate = LocalDate.parse(raw)
      def toInstant = Instant.parse(raw)
      val localDateFormat = raw"(\d{4})-(\d{2})-(\d{2})".r

      try {
        raw match {
          case localDateFormat(_, _, _) =>
            DateOrInstant(raw, None, Some(toLocalDate))
          case _ => DateOrInstant(raw, Some(toInstant), None)
        }
      } catch {
        case e: IllegalArgumentException =>
          logger.error(
            s"Could not parse raw local date or Instant: $raw. Exception: ${e.toString}"
          )
          DateOrInstant(raw, None, None)
        case NonFatal(e) =>
          logger.error(s"Could not parse raw local date or Instant: $raw. Exception: ${e.toString}")
          DateOrInstant(raw, None, None)
      }
    }

    implicit val timestampDecoder: Decoder[Option[DateOrInstant]] =
      Decoder
        .decodeOption[String]
        .map(parseDateOrInstant)
  }
}
