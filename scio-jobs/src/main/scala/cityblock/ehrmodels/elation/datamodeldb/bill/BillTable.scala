package cityblock.ehrmodels.elation
package datamodeldb.bill

import cityblock.ehrmodels.elation.datamodeldb.DatabaseObject
import cityblock.ehrmodels.elation.datamodeldb.practice.PracticeTable.Practice
import cityblock.ehrmodels.elation.datamodeldb.practice.ServiceLocationTable.ServiceLocation
import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDateTime

object BillTable {
  @BigQueryType.toTable
  case class Bill(
    id: Long,
    visit_note_id: Long,
    practice_id: Long,
    notes: Option[String],
    billing_date: Option[LocalDateTime],
    ref_number: Option[String],
    service_location_id: Option[Long],
    place_of_service: Option[Long],
    billing_status: Option[String],
    billing_error: Option[String],
    show_dual_coding: Boolean,
    copay_amount: Option[Double],
    copay_collection_date: Option[LocalDateTime],
    referring_provider: Option[String],
    referring_provider_state: Option[String],
    deferred_bill: Boolean,
    creation_time: Option[LocalDateTime],
    created_by_user_id: Option[Long],
    last_modified: Option[LocalDateTime]
  )

  object Bill extends DatabaseObject {
    override def table: String = "bill_latest"
    override def queryAll: String =
      s"""
         |SELECT
         |  *
         |FROM
         |  `$project.$dataset.$table`
         |""".stripMargin
  }

  case class FullBill(
    bill: Bill,
    practice: Option[Practice],
    serviceLocation: Option[ServiceLocation],
    fullBillItems: Iterable[BillItemTable.FullBillItem]
  )

}
