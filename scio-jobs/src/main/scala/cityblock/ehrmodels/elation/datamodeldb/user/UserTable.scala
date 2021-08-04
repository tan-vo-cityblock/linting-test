package cityblock.ehrmodels
package elation.datamodeldb.user
import cityblock.ehrmodels.elation.datamodeldb.DatabaseObject
import cityblock.ehrmodels.elation.datamodeldb.practice.PracticeTable
import com.spotify.scio.bigquery.types.BigQueryType

object UserTable {
  @BigQueryType.toTable
  case class User(
    id: Long,
    email: String,
    is_active: Boolean,
    practice_id: Long,
    first_name: Option[String],
    last_name: Option[String],
    is_practice_admin: Boolean,
    user_type: String,
    office_staff_id: Option[Long],
    physician_id: Option[Long],
    specialty: Option[String],
    credentials: Option[String],
    npi: Option[String]
  )

  object User extends DatabaseObject {
    override def table: String = "user_latest"
    override def queryAll: String =
      s"""
       |SELECT
       |  *
       |REPLACE(REGEXP_REPLACE(email, r"\\+.*@", "@") AS email)
       |FROM
       |  `$project.$dataset.$table`
       |""".stripMargin
  }

  case class FullUser(user: User, practice: Option[PracticeTable.Practice])
}
