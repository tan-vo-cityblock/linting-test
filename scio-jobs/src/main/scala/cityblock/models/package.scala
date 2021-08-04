package cityblock

import com.spotify.scio.bigquery.types.{description, BigQueryType}

package object models {

  @BigQueryType.toTable
  case class Surrogate(
    id: String,
    project: String,
    dataset: String,
    table: String
  )

  @BigQueryType.toTable
  case class Identifier(
    id: String,
    partner: String,
    surrogate: Surrogate
  )

  @BigQueryType.toTable
  case class SilverIdentifier(
    surrogateId: String,
    @description("Unique composite key that can be used for joins with other silver tables")
    joinId: Option[String]
  )
}
