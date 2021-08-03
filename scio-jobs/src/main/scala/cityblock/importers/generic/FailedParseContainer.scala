package cityblock.importers.generic

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDateTime

object FailedParseContainer {
  @BigQueryType.toTable
  case class FailedParse(tableName: String,
                         errorMsg: String,
                         rawLine: String,
                         timestamp: LocalDateTime = LocalDateTime.now())
}
