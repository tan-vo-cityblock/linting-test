package cityblock.models.connecticare.silver

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object FacetsPharmacyMed {
  @BigQueryType.toTable
  case class ParsedFacetsPharmacyMed(
    claim_number: String,
    claim_status: Option[String],
    member_id: String,
    provider_id: Option[String],
    date_of_service: Option[LocalDate],
    ndc_number: Option[String],
    ndc_desc: Option[String],
    amnt_paid: Option[String],
    amnt_elig: Option[String],
    amnt_charged: Option[String],
    days_supply: Option[String],
    origin_code: Option[String],
    nabp_number: Option[String],
    quantity_dispensed: Option[String],
    paid_date: Option[LocalDate],
    refill_code: Option[String],
    npi_number: Option[String],
    copay_amt: Option[String],
    claim_fee: Option[String],
    claim_deductible: Option[String]
  )
}
