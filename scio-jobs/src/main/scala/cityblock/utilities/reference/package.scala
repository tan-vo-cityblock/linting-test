package cityblock.utilities

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

package object reference {
  val project = "reference-data-199919"

  sealed trait ReferenceTable extends Serializable {
    val dataset: String
    val table: String

    final def tableSpec: String = s"$project.$dataset.$table"
  }

  trait MappingTable[T <: HasAnnotation] extends ReferenceTable {
    def queryAll: String = s"SELECT * FROM `$tableSpec`"
    final def fetchAll(
      sc: ScioContext)(implicit ct: ClassTag[T], tt: TypeTag[T], cd: Coder[T]): SCollection[T] =
      sc.typedBigQuery[T](queryAll)
  }

  @BigQueryType.toTable
  case class ValidCode(code: String)

  trait HasCode {
    def code: String
  }

  trait ValidCodesTable extends ReferenceTable {
    case class Valid(code: String) extends HasCode

    val codeField: String

    def queryDistinct: String =
      s"""
         |SELECT DISTINCT $codeField as code FROM `$tableSpec`
         |WHERE $codeField IS NOT NULL
         |""".stripMargin
    final def fetchDistinct(sc: ScioContext): SCollection[Valid] =
      sc.typedBigQuery[ValidCode](queryDistinct).map(v => Valid(v.code))
  }
}
