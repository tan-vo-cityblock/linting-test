package cityblock.ehrmodels.elation.datamodeldb.practice
import cityblock.ehrmodels.elation.datamodeldb.DatabaseObject
import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDateTime

object ServiceLocationTable {
  @BigQueryType.toTable
  case class ServiceLocation(
    id: Long,
    address: String,
    suite: Option[String],
    city: String,
    state: String,
    zip: String,
    phone: Option[String],
    fax: Option[String],
    practice_id: Long,
    is_primary: Boolean,
    creation_time: Option[LocalDateTime],
    created_by_user_id: Option[Long],
    deletion_time: Option[LocalDateTime],
    deleted_by_user_id: Option[Long]
  )

  object ServiceLocation extends DatabaseObject {
    override def table: String = "service_location_latest"
    override def queryAll: String =
      s"""
       |SELECT
       |  *
       |FROM
       |  `$project.$dataset.$table`
       |WHERE
       | deletion_time IS NULL
       |""".stripMargin
  }
}
