package cityblock.computedfields.common

import com.spotify.scio.bigquery.BigQueryType
import org.joda.time.Instant

object ComputedFieldResults {

  @BigQueryType.toTable
  case class NewQueryResult(
    patientId: Option[String],
    time: Option[Instant],
    value: Option[String],
    fieldSlug: Option[String]
  )

  case class NewComputedFieldResult(
    patientId: String,
    slug: String,
    value: String,
    jobId: String
  )
}
