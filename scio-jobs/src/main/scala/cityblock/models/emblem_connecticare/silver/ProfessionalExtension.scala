package cityblock.models.emblem_connecticare.silver

import com.spotify.scio.bigquery.types.BigQueryType

object ProfessionalExtension {
  @BigQueryType.toTable
  case class ParsedProfessionalExtension(
    CLAIM_ID: String,
    CLM_LN_SEQ_NBR: Option[String],
    DIAG_CD_PRIMARY: Option[String],
    DIAG_CD_02_ADTNL: Option[String],
    DIAG_CD_03_ADTNL: Option[String],
    DIAG_CD_04_ADTNL: Option[String],
    DIAG_CD_05_ADTNL: Option[String],
    DIAG_CD_06_ADTNL: Option[String],
    DIAG_CD_07_ADTNL: Option[String],
    DIAG_CD_08_ADTNL: Option[String],
    DIAG_CD_09_ADTNL: Option[String],
    DIAG_CD_10_ADTNL: Option[String],
    DIAG_CD_11_ADTNL: Option[String],
    DIAG_CD_12_ADTNL: Option[String],
    DIAG_CD_POINTER_02: Option[String],
    DIAG_CD_POINTER_03: Option[String],
    DIAG_CD_POINTER_04: Option[String],
    DIAG_CD_POINTER_05: Option[String],
    DIAG_CD_POINTER_06: Option[String],
    DIAG_CD_POINTER_07: Option[String],
    DIAG_CD_POINTER_08: Option[String],
    POINTER_02: Option[String],
    POINTER_03: Option[String],
    POINTER_04: Option[String],
    POINTER_05: Option[String],
    POINTER_06: Option[String],
    POINTER_07: Option[String],
    POINTER_08: Option[String],
    ICD_VERSION: Option[String]
  )
}
