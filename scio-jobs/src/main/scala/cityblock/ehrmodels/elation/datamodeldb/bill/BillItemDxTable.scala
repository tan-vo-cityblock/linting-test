package cityblock.ehrmodels
package elation.datamodeldb.bill
import cityblock.ehrmodels.elation.datamodeldb.DatabaseObject
import cityblock.ehrmodels.elation.datamodeldb.icd.Icd10Table.Icd10
import cityblock.ehrmodels.elation.datamodeldb.icd.Icd9Table.Icd9
import com.spotify.scio.bigquery.types.BigQueryType

object BillItemDxTable {
  @BigQueryType.toTable
  case class BillItemDx(
    bill_item_id: Long,
    seqno: Long,
    dx: String,
    icd10_id: Long,
    icd9_id: Option[Long]
  )

  object BillItemDx extends DatabaseObject {
    override def table: String = "bill_item_dx_latest"
    override def queryAll: String =
      s"""
       |SELECT
       |  *
       |FROM
       |  `$project.$dataset.$table`
       |""".stripMargin
  }

  case class FullBillItemDx(billItemDx: BillItemDx, icd10: Option[Icd10], icd9: Option[Icd9])
}
