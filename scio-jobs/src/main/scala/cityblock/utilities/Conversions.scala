package cityblock.utilities

import java.time.{
  Instant => JavaInstant,
  LocalDate => JavaLocalDate,
  LocalDateTime => JavaLocalDateTime
}

import cityblock.utilities.time._
import org.joda.time.{DateTimeZone => JodaDateTimeZone, LocalDateTime => JodaLocalDateTime}
import squants.mass.{Kilograms, Pounds}
import squants.thermal.{Celsius, Fahrenheit}
import squants.space.{Centimeters, Inches}

import scala.util.control.NonFatal

object Conversions {
  def removeEmptyString(s: String): Option[String] =
    Some(s).map(_.trim).filter(_.nonEmpty)

  def removeEmptyString(s: Option[String]): Option[String] =
    s.map(_.trim).filter(_.nonEmpty)

  def stripLeadingZeros(s: Option[String], finalLength: Int): Option[String] =
    s.map(string => {
      val originalLength: Int = string.length
      string.substring(originalLength - finalLength, originalLength)
    })

  def mkDateOrInstant(instant: Option[JavaInstant]): Option[DateOrInstant] =
    instant match {
      case Some(time) => Some(mkDateOrInstant(time))
      case _          => None
    }

  def mkDateOrInstant(instant: JavaInstant): DateOrInstant =
    DateOrInstant(instant.toString, Some(instant.toJoda), None)

  def mkDateOrInstant(dateTime: JavaLocalDateTime): DateOrInstant =
    DateOrInstant(dateTime.toString, Some(dateTime.toJoda()), None)

  def mkDateOrInstant(date: JavaLocalDate): DateOrInstant =
    DateOrInstant(date.toString, None, Some(date.toJoda()))

  def mkDateOrInstant(
    localDateTime: Option[JodaLocalDateTime],
    jodaTimeZoneID: String
  ): Option[DateOrInstant] =
    localDateTime.fold(Option.empty[DateOrInstant]) { time =>
      Some(mkDateOrInstant(time, jodaTimeZoneID))
    }

  // Full list of joda timezone ids: http://joda-time.sourceforge.net/timezones.html
  def mkDateOrInstant(localDateTime: JodaLocalDateTime, jodaTimeZoneID: String): DateOrInstant = {
    val instant = localDateTime.toDateTime(JodaDateTimeZone.forID(jodaTimeZoneID)).toInstant
    DateOrInstant(localDateTime.toString, Some(instant), None)
  }

  def inchesToCm(in: Double): String = {
    val inches = Inches(in)
    val cmTup = inches toTuple Centimeters
    precisionFormat(cmTup._1)
  }

  def lbsToKg(lbs: Double): String = {
    val pounds = Pounds(lbs)
    val kgTup = pounds toTuple Kilograms
    precisionFormat(kgTup._1)
  }

  def fahrenheitToCel(fahrenheit: Double): String = {
    val fah = Fahrenheit(fahrenheit)
    val celTup = fah toTuple Celsius
    precisionFormat(celTup._1)
  }

  private def precisionFormat(value: Double): String =
    "%.3f".format(value)

  /**
   * Try to parse a string with a function that throws exception, and return [[None]] if the
   * parsing fails.
   * @param s string to parse
   * @param parse parsing function (that may throw an exception)
   * @tparam T parsing function destination type
   * @return the parsed value, or [[None]]
   */
  def safeParse[T](s: String, parse: String => T): Option[T] =
    try {
      Some(parse(s))
    } catch {
      case NonFatal(_) => None
    }

  /**
   *
   * @param caseClass case class to decompose into a map
   * @return mapping of case class field names to their values. Useful to programmatically access a case class value
   *         without explicitly knowing the name of the corresponding field.
   *         Credit: https://stackoverflow.com/questions/1226555/case-class-to-map-in-scala/1227643#1227643
   */
  def getCaseClassParams(caseClass: AnyRef): Map[String, Any] =
    caseClass.getClass.getDeclaredFields.foldLeft(Map.empty[String, Any]) { (mapping, field) =>
      field.setAccessible(true)
      mapping + (field.getName -> field.get(caseClass))
    }

  def isAllNones[T](values: List[Option[T]]): Boolean = !values.exists(_.isDefined)

  def getValueForField(field: String, map: Map[String, Any]): Option[String] =
    map.get(field).flatMap {
      case code: Option[_] => code.map(_.toString)
      case _               => None
    }
}
