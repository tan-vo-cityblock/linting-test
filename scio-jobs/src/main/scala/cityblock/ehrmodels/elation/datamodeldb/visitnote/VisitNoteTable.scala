package cityblock.ehrmodels.elation
package datamodeldb.visitnote

import cityblock.ehrmodels.elation.datamodeldb.DatabaseObject
import cityblock.ehrmodels.elation.datamodeldb.bill.BillTable.FullBill
import cityblock.ehrmodels.elation.datamodeldb.practice.PracticeTable.Practice
import cityblock.ehrmodels.elation.datamodeldb.user.UserTable.FullUser
import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDateTime

object VisitNoteTable {
  @BigQueryType.toTable
  case class VisitNote(
    id: Long,
    patient_id: Long,
    practice_id: Long,
    name: Option[String],
    document_date: LocalDateTime,
    chart_feed_date: LocalDateTime,
    physician_user_id: Option[Long],
    last_modified: Option[LocalDateTime],
    creation_time: Option[LocalDateTime],
    created_by_user_id: Option[Long],
    deletion_time: Option[LocalDateTime],
    deleted_by_user_id: Option[Long],
    signed_time: Option[LocalDateTime],
    signed_by_user_id: Option[Long],
    from_plr: Long,
    transition_of_care: Option[String],
    meds_reconciled: Option[Boolean],
    current_meds_documented: Option[Boolean]
  )

  object VisitNote extends DatabaseObject {
    override def table: String = "visit_note_latest"
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

  case class FullVisitNote(
    visitNote: VisitNote,
    visitNoteBullets: Iterable[VisitNoteBulletTable.VisitNoteBullet],
    fullBill: Option[FullBill],
    practice: Option[Practice],
    fullUser: Option[FullUser]
  )
}
