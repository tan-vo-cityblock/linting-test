package cityblock.ehrmodels.elation
package datamodeldb.nonvisitnote

import cityblock.ehrmodels.elation.datamodeldb.DatabaseObject
import cityblock.ehrmodels.elation.datamodeldb.practice.PracticeTable.Practice
import cityblock.ehrmodels.elation.datamodeldb.user.UserTable.FullUser
import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDateTime

object NonVisitNoteTable {
  @BigQueryType.toTable
  case class NonVisitNote(
    id: Long,
    patient_id: Long,
    practice_id: Long,
    note_type: Option[String],
    document_date: LocalDateTime,
    chart_feed_date: LocalDateTime,
    last_modified: Option[LocalDateTime],
    creation_time: Option[LocalDateTime],
    created_by_user_id: Option[Long],
    deletion_time: Option[LocalDateTime],
    deleted_by_user_id: Option[Long],
    signed_time: Option[LocalDateTime],
    signed_by_user_id: Option[Long],
    from_plr: Long
  )

  object NonVisitNote extends DatabaseObject {
    override def table: String = "non_visit_note_latest"
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

  case class FullNonVisitNote(
    nonVisitNote: NonVisitNote,
    nonVisitNoteBullets: Iterable[NonVisitNoteBulletTable.NonVisitNoteBullet],
    fullNonVisitNoteDocumentTag: Iterable[NonVisitNoteDocumentTagTable.FullNonVisitNoteDocumentTag],
    practice: Option[Practice],
    fullUser: Option[FullUser]
  )
}
