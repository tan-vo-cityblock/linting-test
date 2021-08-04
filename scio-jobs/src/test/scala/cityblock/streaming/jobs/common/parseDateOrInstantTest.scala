package cityblock.streaming.jobs.common

import com.spotify.scio.testing.PipelineSpec
import org.joda.time.{Instant, LocalDate}

class parseDateOrInstantTest extends PipelineSpec with ParsableData {
  final val DATE = "1993-06-10"
  final val INSTANT = "1993-06-10T06:30:00.000Z"

  "common.parseDateOrInstant" should "parse an empty Option as None" in {
    parseDateOrInstant(None) should equal(None)
  }

  "common.parseDateOrInstant" should "parse an empty String as None" in {
    parseDateOrInstant(Some("")) should equal(None)
  }

  "common.parseDateOrInstant" should "parse an incorrectly formatted date as None" in {
    parseDateOrInstant(Some("This is definitely not a date")) should equal(None)
  }

  "common.parseDateOrInstant" should "parse a date lower than Standard SQL min timestamp as None" in {
    parseDateOrInstant(Some("0000-01-01T00:00:00.000Z")) should equal(None)
  }

  "common.parseDateOrInstant" should "parse a date higher than Standard SQL max timestamp as None" in {
    parseDateOrInstant(Some("10000-01-01T00:00:00.0000Z")) should equal(None)
  }

  "common.parseDateOrInstant" should "parse a valid date as a Date DateOrInstant" in {
    val doi = parseDateOrInstant(Some(DATE)).get
    doi should not equal (null)
    doi.raw should equal(DATE)
    doi.instant should equal(None)

    val date = doi.date.get
    date should not equal (null)
    date should equal(new LocalDate(DATE))
  }

  "common.parseDateOrInstant" should "parse a valid instant as an Instant DateOrInstant" in {
    val doi = parseDateOrInstant(Some(INSTANT)).get
    doi should not equal (null)
    doi.raw should equal(INSTANT)
    doi.date should equal(None)

    val instant = doi.instant.get
    instant should equal(new Instant(INSTANT))
  }
}
