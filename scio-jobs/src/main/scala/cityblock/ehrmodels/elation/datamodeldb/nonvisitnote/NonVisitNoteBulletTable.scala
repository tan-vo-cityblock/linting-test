package cityblock.ehrmodels.elation.datamodeldb.nonvisitnote
import cityblock.ehrmodels.elation.datamodeldb.DatabaseObject
import com.spotify.scio.bigquery.types.BigQueryType

object NonVisitNoteBulletTable {
  @BigQueryType.toTable
  case class NonVisitNoteBullet(id: Long, non_visit_note_id: Long, sequence: Long, text: String)

  object NonVisitNoteBullet extends DatabaseObject {
    override def table: String = "non_visit_note_bullet_latest"
    override def queryAll: String =
      s"""
       |SELECT
       |  *
       |FROM
       |  `$project.$dataset.$table`
       |""".stripMargin
  }
}
