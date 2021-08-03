package cityblock.models.connecticare.silver

import com.spotify.scio.bigquery.types.BigQueryType

object UBHDiagnosis {
  @BigQueryType.toTable
  case class ParsedUBHDiagnosis(
    AUDNBR_Header: String,
    DiagnosisIdNum: String,
    DiagnosisNbr: Int,
    DiagnosisTypeDesc: Option[String],
    ICDVersion: Option[String],
    LOB: Option[String]
  )
}
