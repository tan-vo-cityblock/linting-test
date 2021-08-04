package cityblock.models.connecticare.silver

import com.spotify.scio.bigquery.types.BigQueryType

object MedicalICDProcedure {

  @BigQueryType.toTable
  case class ParsedMedicalICDProcedure(
    ClmNum: String,
    ICDProcCategory: Option[String],
    ICDProcCd: Option[String],
    ICD_Indicator: Option[String]
  )
}
