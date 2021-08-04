package cityblock.utilities

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.Instant

object PubSub {
  @BigQueryType.toTable
  case class PubSubMessage(
    insertedAt: Instant = Instant.now,
    topic: String,
    topicShort: Option[String],
    hmac: Option[String],
    payload: String,
  )
}
