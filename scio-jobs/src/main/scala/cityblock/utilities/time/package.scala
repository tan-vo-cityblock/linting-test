package cityblock.utilities

import java.time.{
  ZoneOffset,
  Instant => JavaInstant,
  LocalDate => JavaLocalDate,
  LocalDateTime => JavaLocalDateTime
}

import com.spotify.scio.bigquery.types.BigQueryType
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import kantan.xpath.NodeDecoder
import kantan.xpath.literals.XPathStringContext
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTimeZone, Instant, LocalDate}

import scala.util.Try

package object time {
  @BigQueryType.toTable
  case class DateOrInstant(raw: String, instant: Option[Instant], date: Option[LocalDate])

  object DateOrInstant {
    implicit val dateOrInstantEncoder: Encoder[DateOrInstant] = Encoder.instance {
      case DateOrInstant(_, Some(instant), _) => instant.toString.asJson
      case DateOrInstant(_, None, Some(date)) => date.toString.asJson
      case DateOrInstant(raw, _, _)           => raw.asJson
    }

    implicit val dateOrInstantDecoder: NodeDecoder[DateOrInstant] =
      NodeDecoder.decoder(xp"./@value") { timestamp: String =>
        createDateOrInstant(timestamp).get
      }
  }

  implicit class RichJavaInstant(instant: JavaInstant) {
    def toJoda: org.joda.time.Instant =
      new Instant(instant.toEpochMilli)
  }

  implicit class RichJavaLocalDate(javaLocalDate: JavaLocalDate) {
    def toJoda(
      zoneOffset: ZoneOffset = ZoneOffset.UTC,
      dateTimeZone: DateTimeZone = DateTimeZone.UTC
    ): LocalDate = {
      val epochMilli =
        javaLocalDate.atStartOfDay.atZone(zoneOffset).toInstant.toEpochMilli
      new LocalDate(epochMilli, dateTimeZone)
    }
  }

  implicit class RichJavaLocalDateTime(javaLocalDateTime: JavaLocalDateTime) {
    def toJoda(zoneOffset: ZoneOffset = ZoneOffset.UTC): Instant =
      javaLocalDateTime.toInstant(zoneOffset).toJoda
  }

  implicit class RichJodaLocalDate(jodaLocalDate: LocalDate) {
    def toJavaDate(
      dateTimeZone: DateTimeZone = DateTimeZone.UTC,
      zoneOffset: ZoneOffset = ZoneOffset.UTC
    ): JavaLocalDate = {
      val epochMilli =
        jodaLocalDate.toDateTimeAtStartOfDay(dateTimeZone).getMillis
      JavaInstant.ofEpochMilli(epochMilli).atZone(zoneOffset).toLocalDate
    }
  }

  lazy val jodaLocalDateDecoder: Decoder[LocalDate] = Decoder.decodeString
    .emapTry { str =>
      Try(LocalDate.parse(str))
    }

  lazy val jodaLocalDateEncoder: Encoder[LocalDate] = Encoder.encodeString
    .contramap[LocalDate](_.toString())

  private def createDateOrInstant(xmlDateRaw: String): Option[DateOrInstant] = {
    val ccdaDateFormat: DateTimeFormatter =
      DateTimeFormat.forPattern("yyyyMMdd")
    xmlDateRaw match {
      case "" => None

      case dateRaw =>
        val localStartDate = LocalDate.parse(dateRaw, ccdaDateFormat)
        Some(DateOrInstant(raw = dateRaw, instant = None, date = Some(localStartDate)))
    }
  }
}
