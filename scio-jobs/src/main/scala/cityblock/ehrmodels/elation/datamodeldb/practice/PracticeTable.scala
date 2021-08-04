package cityblock.ehrmodels.elation.datamodeldb.practice
import cityblock.ehrmodels.elation.datamodeldb.DatabaseObject
import com.spotify.scio.bigquery.types.BigQueryType

object PracticeTable {
  @BigQueryType.toTable
  case class Practice(
    id: Long,
    name: String,
    address_line1: String,
    address_line2: Option[String],
    city: String,
    state: String,
    zip: String,
    phone: Option[String],
    fax: Option[String]
  )

  object Practice extends DatabaseObject {
    override def table: String = "practice_latest"
    override def queryAll: String =
      s"""
       |SELECT
       |  *
       |FROM
       |  `$project.$dataset.$table`
       |""".stripMargin
  }
}
