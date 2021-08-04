package cityblock.member.attribution.transforms

import org.joda.time.LocalDate

object Utils {
  private val TODAY: LocalDate = LocalDate.now()

  def dateOfDemiseTransform(dateOfDemise: Option[LocalDate]): Option[LocalDate] =
    dateOfDemise.filter(_.isBefore(new LocalDate()))

  def lastFourSSN(socialSecurityNumber: Option[String]): Option[String] =
    socialSecurityNumber.map(_.takeRight(4))

  def isCurrent(
    spanDateStart: Option[LocalDate],
    spanDateEnd: Option[LocalDate]
  ): Boolean = {
    val noSpanStart = spanDateStart.isEmpty
    val noSpanEnd = spanDateEnd.isEmpty
    // TODO: Update this to take into account desired run-time
    val spanEndBeforeToday = spanDateEnd.exists(_.compareTo(TODAY) <= 0)

    noSpanStart || noSpanEnd || !spanEndBeforeToday
  }
}
