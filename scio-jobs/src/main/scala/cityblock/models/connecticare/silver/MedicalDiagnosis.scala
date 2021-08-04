package cityblock.models.connecticare.silver

import com.spotify.scio.bigquery.types.BigQueryType

object MedicalDiagnosis {

  @BigQueryType.toTable
  case class ParsedMedicalDiagnosis(
    ClmNum: String,
    DiagCategory: Option[String],
    DiagCd: Option[String],
    ICD_Indicator: Option[String]
  )
}
