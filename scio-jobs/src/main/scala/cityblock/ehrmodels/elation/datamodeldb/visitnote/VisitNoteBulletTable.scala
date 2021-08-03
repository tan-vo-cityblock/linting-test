package cityblock.ehrmodels.elation.datamodeldb.visitnote

import cityblock.ehrmodels.elation.datamodeldb.DatabaseObject
import com.spotify.scio.bigquery.types.BigQueryType

object VisitNoteBulletTable {
  @BigQueryType.toTable
  case class VisitNoteBullet(
    id: Long,
    visit_note_id: Long,
    parent_bullet_id: Option[Long],
    category: String,
    sequence: Long,
    text: String
  )

  object VisitNoteBullet extends DatabaseObject {
    override def table: String = "visit_note_bullet_latest"
    override def queryAll: String =
      s"""
       |SELECT
       |  *
       |FROM
       |  `$project.$dataset.$table`
       |WHERE
       | deleted_date IS NULL
       |""".stripMargin
  }
}
