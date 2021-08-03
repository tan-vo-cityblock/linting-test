package cityblock.ehrmodels.elation.datamodeldb.bill

import cityblock.ehrmodels.elation.datamodeldb.DatabaseObject
import cityblock.ehrmodels.elation.datamodeldb.bill.BillItemDxTable.FullBillItemDx
import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDateTime

object BillItemTable {
  @BigQueryType.toTable
  case class BillItem(
    id: Long,
    bill_id: Long,
    cpt: String,
    seqno: Long,
    last_modified: Option[LocalDateTime],
    modifier_1: Option[String],
    modifier_2: Option[String],
    modifier_3: Option[String],
    modifier_4: Option[String],
    units: Option[Double],
    unit_charge: Option[Double],
    creation_time: Option[LocalDateTime],
    created_by_user_id: Option[Long],
    deletion_time: Option[LocalDateTime],
    deleted_by_user_id: Option[Long]
  )

  object BillItem extends DatabaseObject {
    override def table: String = "bill_item_latest"
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

  case class FullBillItem(billItem: BillItem, diagnoses: Iterable[FullBillItemDx])
}
