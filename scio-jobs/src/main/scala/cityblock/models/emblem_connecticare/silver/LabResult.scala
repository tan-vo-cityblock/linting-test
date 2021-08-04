package cityblock.models.emblem_connecticare.silver

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object LabResult {
  @BigQueryType.toTable
  case class ParsedLabResult(
    PARENT_COMPANY_CD: Option[String],
    EDM_LAB_RESULT_KEY: String,
    LAB_VNDR_NM: Option[String],
    LAB_CD: Option[String],
    SVC_DT: Option[LocalDate],
    UNIQUE_PERSON_ID: Option[String],
    MED_EMP_GRP_ID: Option[String],
    MED_PLN_ID: Option[String],
    MBR_ID: Option[String],
    PAT_FRST_NM: Option[String],
    PAT_MIDL_NM: Option[String],
    PAT_LST_NM: Option[String],
    PAT_ADR_LN_1: Option[String],
    PAT_ADR_LN_2: Option[String],
    PAT_CITY: Option[String],
    PAT_ST: Option[String],
    PAT_ZIP: Option[String],
    PAT_PHON: Option[String],
    PAT_DOB: Option[LocalDate],
    PAT_GNDR_CD: Option[String],
    RFR_PRVD_NM: Option[String],
    RFR_PRVD_ID: Option[String],
    NATL_PRVD_ID: Option[String],
    ORDR_ACCT_NBR: Option[String],
    ORDR_ACCT_NM: Option[String],
    ORDR_ACCT_ADR_LN_1: Option[String],
    ORDR_ACCT_ADR_LN_2: Option[String],
    ORDR_ACCT_CITY: Option[String],
    ORDR_ACCT_ST: Option[String],
    ORDR_ACCT_ZIP: Option[String],
    ORDR_ACCT_PHON: Option[String],
    LCL_ORDR_ID: Option[String],
    LCL_ORDR_NM: Option[String],
    LCL_RSLT_CD: Option[String],
    LCL_RSLT_NM: Option[String],
    LCL_RSLT_VAL_1: Option[String],
    LCL_RSLT_VAL_2: Option[String],
    RSLT_UNIT: Option[String],
    REF_RNG_MIN_VAL: Option[String],
    REF_RNG_MAX_VAL: Option[String],
    LCL_INCDT_CD: Option[String],
    CURR_PROC_TRMLGY_CD: Option[String],
    DIAG_CD_1: Option[String],
    DIAG_CD_2: Option[String],
    DIAG_CD_3: Option[String]
  )
}
