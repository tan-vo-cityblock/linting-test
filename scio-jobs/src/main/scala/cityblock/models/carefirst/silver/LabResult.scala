package cityblock.models.carefirst.silver

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object LabResult {
  @BigQueryType.toTable
  case class ParsedLabResult(
    EXTERNAL_ID: Option[String],
    MEM_ID: String,
    MEM_LNAME: Option[String],
    MEM_FNAME: Option[String],
    MEM_DOB: Option[LocalDate],
    LAB_TEST_ID: Option[String],
    TEST_DATE: Option[LocalDate],
    RESULT_DATE: Option[LocalDate],
    ICD_VERSION: Option[String],
    DIAG_CODE: Option[String],
    TIER: Option[String],
    CODESET: Option[String],
    PROCCODE1: Option[String],
    PROCCODE2: Option[String],
    LOCALORDERCODE: Option[String],
    LOINC: Option[String],
    LOCAL_TEST_NAME: Option[String],
    LOCAL_TEST_RESULT: Option[String],
    LOCAL_TEST_RESULT_NUM: Option[String],
    LOCAL_TEST_UNITS: Option[String],
    RESULT_NUM: Option[String],
    RESULT_NUM_LOW: Option[String],
    RESULT_NUM_HIGH: Option[String],
    RESULT_RANGE: Option[String],
    ABNORMAL_FLAG: Option[String],
    NAT_ORDER_CODE: Option[String],
    NAT_RESULT_CODE: Option[String],
    PROCESSING_ENTITY: Option[String],
    SITE_ID: Option[String],
    SITE_NAME: Option[String]
  )
}
