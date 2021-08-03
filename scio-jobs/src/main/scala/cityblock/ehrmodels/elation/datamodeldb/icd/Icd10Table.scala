package cityblock.ehrmodels.elation.datamodeldb.icd
import cityblock.ehrmodels.elation.datamodeldb.DatabaseObject
import com.spotify.scio.bigquery.types.BigQueryType

object Icd10Table {
  @BigQueryType.toTable
  case class Icd10(id: Long, code: String, description: String)

  object Icd10 extends DatabaseObject {
    override def table: String = "icd10_latest"
    override def queryAll: String =
      s"""
       |SELECT
       |  *
       |FROM
       |  `$project.$dataset.$table`
       |""".stripMargin
  }
}
