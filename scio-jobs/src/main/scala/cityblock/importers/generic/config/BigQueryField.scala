package cityblock.importers.generic.config

import cityblock.importers.generic.config.BigQueryFieldType.BigQueryFieldType
import cityblock.utilities.Loggable
import com.google.api.services.bigquery.model.TableFieldSchema
import io.circe.Decoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveDecoder
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.joda.time.{DateTime, DateTimeZone, LocalDateTime}

import scala.language.implicitConversions

case class BigQueryField(
  // The name of the field
  name: String,
  // A description of the field
  description: Option[String] = None,
  // The type of this field. All types except RECORD are supported
  fieldType: BigQueryFieldType.BigQueryFieldType = BigQueryFieldType.STRING,
  // Whether or not this field is required to be present
  required: Boolean = false
) extends Loggable {
  import BigQueryField._

  def tableFieldSchema: TableFieldSchema =
    new TableFieldSchema()
      .setName(name)
      .setDescription(description.getOrElse(""))
      .setType(fieldType)
      .setMode(BigQueryFieldMode.getMode(required))

  def validate(): Unit =
    require(fieldType != BigQueryFieldType.RECORD, "RECORD is not allowed as field type")

  def normalize(raw: String, trimWhitespace: Boolean = true): Option[String] = {
    val maybeTrimmed: String =
      if (fieldType == BigQueryFieldType.STRING && !trimWhitespace) raw else raw.trim
    val normalized: Option[String] = fieldType match {
      case BigQueryFieldType.DATE | BigQueryFieldType.DATETIME | BigQueryFieldType.TIME |
          BigQueryFieldType.TIMESTAMP =>
        normalizeDate(maybeTrimmed, fieldType)
      case BigQueryFieldType.BOOLEAN =>
        normalizeBoolean(maybeTrimmed)
      case BigQueryFieldType.NUMERIC =>
        try {
          val numberVal = if (maybeTrimmed == "N") "0.00" else maybeTrimmed
          Option(numberVal.replace("$", "").toDouble.toString)
        } catch {
          case _: Exception => None
        }
      case _ => Option(maybeTrimmed).filter(_.toLowerCase != "null")
    }

    normalized.filter(_.nonEmpty)
  }

  private def normalizeBoolean(trimmed: String): Option[String] = trimmed match {
    case "t" | "true" | "T" | "Y" | "y" | "1"  => Some("true")
    case "f" | "false" | "F" | "N" | "n" | "0" => Some("false")
    case _                                     => None
  }

  // scalastyle:off cyclomatic.complexity
  private def normalizeDate(trimmed: String, fieldType: BigQueryFieldType): Option[String] = {
    val parsedLocalDate: Option[LocalDateTime] =
      try {
        trimmed match {
          case TimestampPattern(_*) =>
            Some(LocalDateTime.parse(trimmed ++ " UTC", TimestampFormatter))
          case LocalDateTimeMonthDayYearSlashesPattern(_*) =>
            Some(LocalDateTime.parse(trimmed, LocalDateTimeMonthDayYearSlashesFormatter))
          case YearMonthPattern(_*) =>
            Some(LocalDateTime.parse(trimmed, YearMonthFormatter))
          case PgTimestampWithZonePattern(_*) =>
            Some(LocalDateTime.parse(trimmed, PgTimestampWithZoneFormatter))
          case PgTimestampNoMillisWithZonePattern(_*) =>
            Some(LocalDateTime.parse(trimmed, PgTimestampNoMillisWithZoneFormatter))
          case YearMonthDayPattern(_*) =>
            Some(LocalDateTime.parse(trimmed, YearMonthDayFormatter))
          case DayThreeLetterMonthYearPattern(_*) =>
            Some(LocalDateTime.parse(trimmed, DayThreeLetterMonthYearFormatter))
          case MonthDateShortYearPattern(_*) =>
            Some(LocalDateTime.parse(trimmed, MonthDateShortYearFormatter))
          case TimestampWithMillsNoZonePattern(_*) =>
            Some(LocalDateTime.parse(trimmed, TimestampWithMillsNoZoneFormatter))
          case MonthDateLongYearPattern(_*) =>
            Some(LocalDateTime.parse(trimmed, MonthDateLongYearFormatter))
          case TimestampWithSeparatorsPattern(_*) =>
            Some(DateTime.parse(trimmed)).map(_.toLocalDateTime)
          case MonthDayYearFormatPattern(_*) =>
            Some(LocalDateTime.parse(trimmed, MonthDayYearFormatter))
          case _ => Some(LocalDateTime.parse(trimmed))
        }
      } catch {
        case _: Exception => None
      }

    fieldType match {
      case BigQueryFieldType.DATE =>
        parsedLocalDate.map(ISODateTimeFormat.yearMonthDay().print)
      case BigQueryFieldType.DATETIME =>
        parsedLocalDate.map(ISODateTimeFormat.dateTimeNoMillis().print)
      case BigQueryFieldType.TIME =>
        parsedLocalDate.map(ISODateTimeFormat.timeNoMillis().print)
      case BigQueryFieldType.TIMESTAMP =>
        parsedLocalDate.map(_.toDateTime(DateTimeZone.UTC).getMillis / 1000).map(_.toString)
    }
  }
}

object BigQueryField {
  implicit val customConfig: Configuration = Configuration.default.withDefaults
  implicit val bigQueryFieldDecoder: Decoder[BigQueryField] =
    deriveDecoder[BigQueryField]

  private lazy val TimestampPattern =
    "\\A\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\z".r
  private lazy val TimestampFormatter =
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss z")

  private lazy val MonthDateShortYearPattern = "\\d{1,2}/\\d{1,2}/\\d{1,2}".r
  private lazy val MonthDateShortYearFormatter = DateTimeFormat.forPattern("MM/dd/yy")

  private lazy val LocalDateTimeMonthDayYearSlashesPattern =
    "\\A\\d{2}/\\d{2}/\\d{4} \\d{2}:\\d{2}:\\d{2}".r
  private lazy val LocalDateTimeMonthDayYearSlashesFormatter =
    DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss")

  private lazy val MonthDateLongYearPattern = "\\d{1,2}/\\d{1,2}/\\d{1,4}".r
  private lazy val MonthDateLongYearFormatter = DateTimeFormat.forPattern("MM/dd/yyyy")

  private lazy val YearMonthPattern = "\\A\\d{6}\\z".r
  private lazy val YearMonthFormatter = DateTimeFormat.forPattern("yyyyMM")

  private lazy val YearMonthDayPattern = "\\A\\d{8}\\z".r
  private lazy val YearMonthDayFormatter = DateTimeFormat.forPattern("yyyyMMdd")

  private lazy val PgTimestampWithZonePattern =
    "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{0,8}\\+\\d{2}(?:\\d{2})?".r
  private lazy val PgTimestampWithZoneFormatter =
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSZZ")

  private lazy val PgTimestampNoMillisWithZonePattern =
    "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\+\\d{2}(?:\\d{2})?".r
  private lazy val PgTimestampNoMillisWithZoneFormatter =
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssZZ")

  private lazy val DayThreeLetterMonthYearPattern =
    "^(([0-9])|([0-2][0-9])|([3][0-1]))-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-(\\d{2})$".r
  private lazy val DayThreeLetterMonthYearFormatter =
    DateTimeFormat.forPattern("dd-MMM-yy")

  private lazy val TimestampWithMillsNoZonePattern =
    "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{0,9}?".r
  private lazy val TimestampWithMillsNoZoneFormatter =
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")

  private lazy val TimestampWithSeparatorsPattern =
    "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z".r

  private lazy val MonthDayYearFormatPattern = "\\d{2}-\\d{2}-\\d{4}".r
  private lazy val MonthDayYearFormatter = DateTimeFormat.forPattern("MM-dd-yyyy")
}

object BigQueryFieldType extends Enumeration {
  type BigQueryFieldType = Value

  implicit val customConfig: Configuration = Configuration.default.withDefaults
  implicit val fieldTypedDecoder: Decoder[BigQueryFieldType.Value] =
    Decoder.enumDecoder(BigQueryFieldType)

  implicit def convertToString(fieldType: BigQueryFieldType): String =
    fieldType.toString

  val BOOLEAN = Value
  val BYTES = Value
  val DATE = Value
  val DATETIME = Value
  val FLOAT = Value
  val INTEGER = Value
  val RECORD = Value
  val STRING = Value
  val TIME = Value
  val TIMESTAMP = Value
  val NUMERIC = Value
}

object BigQueryFieldMode extends Enumeration {
  type BigQueryFieldMode = Value

  implicit def convertToString(fieldMode: BigQueryFieldMode): String =
    fieldMode.toString

  def getMode(required: Boolean): BigQueryFieldMode =
    if (required) {
      REQUIRED
    } else {
      NULLABLE
    }

  val NULLABLE = Value
  val REQUIRED = Value
}
