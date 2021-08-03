package cityblock.member.attribution.types

import org.joda.time.LocalDate

object Cardinal {
  // Specific to transformer implementation, not Cardinal data models. Used to de-dupe lines that only differ in
  // subline of business spans.
  case class InsuranceAndDate(
    SubLineOfBusiness: Option[String],
    EnrollmentDate: Option[LocalDate],
    TerminationDate: Option[LocalDate]
  )
}
