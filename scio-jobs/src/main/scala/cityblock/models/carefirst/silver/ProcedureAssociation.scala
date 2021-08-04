package cityblock.models.carefirst.silver

import com.spotify.scio.bigquery.types.BigQueryType

object ProcedureAssociation {
  @BigQueryType.toTable
  case class ParsedProcedureAssociation(
    CLAIM_NUM: String,
    CLAIM_LINE_NUM: Option[String],
    ICD_PROC_CODE: Option[String],
    ICD_VERS_IND: Option[String],
    CLAIM_TYPE: Option[String],
    ICD_PROC_CODE_FORMATTED: Option[String]
  )

}
