package cityblock.ehrmodels.elation.datamodeldb.visitnote
import cityblock.ehrmodels.elation.datamodeldb.DatabaseObject
import com.spotify.scio.bigquery.types.BigQueryType

object VisitNoteDocumentTagTable {
  @BigQueryType.toTable
  case class VisitNoteDocumentTag(
    visit_note_id: Long,
    document_tag_id: Long
  )

  object VisitNoteDocumentTag extends DatabaseObject {
    override def table: String = "visit_note_document_tag_latest"
    override def queryAll: String =
      s"""
         |SELECT
         |  *
         |FROM
         |  `$project.$dataset.$table`
         |""".stripMargin
  }
}
