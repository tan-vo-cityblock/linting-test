package cityblock.ehrmodels.elation.datamodeldb.documenttag
import cityblock.ehrmodels.elation.datamodeldb.DatabaseObject
import com.spotify.scio.bigquery.types.BigQueryType

object DocumentTagTable {
  @BigQueryType.toTable
  case class DocumentTag(id: Long,
                         practice_id: Option[Long],
                         value: String,
                         description: Option[String],
                         code_type: Option[String],
                         code: Option[String],
                         concept_name: Option[String])

  object DocumentTag extends DatabaseObject {
    override def table: String = "document_tag_latest"
    override def queryAll: String =
      s"""
         |SELECT
         |  *
         |FROM
         |  `$project.$dataset.$table`
         |""".stripMargin
  }

}
