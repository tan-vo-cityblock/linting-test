package cityblock.ehrmodels
package elation.datamodeldb.nonvisitnote
import cityblock.ehrmodels.elation.datamodeldb.DatabaseObject
import cityblock.ehrmodels.elation.datamodeldb.documenttag.DocumentTagTable.DocumentTag
import com.spotify.scio.bigquery.types.BigQueryType

object NonVisitNoteDocumentTagTable {
  @BigQueryType.toTable
  case class NonVisitNoteDocumentTag(
    non_visit_note_id: Long,
    document_tag_id: Long
  )

  object NonVisitNoteDocumentTag extends DatabaseObject {
    override def table: String = "non_visit_note_document_tag_latest"
    override def queryAll: String =
      s"""
         |SELECT
         |  *
         |FROM
         |  `$project.$dataset.$table`
         |""".stripMargin
  }

  case class FullNonVisitNoteDocumentTag(nonVisitNoteDocumentTag: NonVisitNoteDocumentTag,
                                         documentTag: DocumentTag)
}
