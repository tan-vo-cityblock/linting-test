package cityblock.models.carefirst.silver

import com.spotify.scio.bigquery.types.BigQueryType

object DiagnosisAssociation {
  @BigQueryType.toTable
  case class ParsedDiagnosisAssociation(
    CLAIM_NUM: String,
    CLAIM_LINE_NUM: Option[String],
    ICD_DIAG_CODE: Option[String],
    ICD_VERS_IND: Option[String],
    ICD_POA_IND: Option[String],
    CLAIM_TYPE: Option[String],
    ICD_DIAG_CODE_FORMATTED: Option[String]
  )
}
