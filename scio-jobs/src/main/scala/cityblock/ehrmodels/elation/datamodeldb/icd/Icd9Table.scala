package cityblock.ehrmodels.elation.datamodeldb.icd
import cityblock.ehrmodels.elation.datamodeldb.DatabaseObject
import com.spotify.scio.bigquery.types.BigQueryType

object Icd9Table {
  @BigQueryType.toTable
  case class Icd9(id: Long, code: String, description: String)

  object Icd9 extends DatabaseObject {
    override def table: String = "icd9_latest"
    override def queryAll: String =
      s"""
       |SELECT
       |  *
       |FROM
       |  `$project.$dataset.$table`
       |""".stripMargin
  }
}
