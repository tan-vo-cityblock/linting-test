package cityblock.parsers.emblem.associations

import com.spotify.scio.bigquery.types.BigQueryType

object DiagnosisAssociationCohort {

  @BigQueryType.toTable
  case class ParsedDiagnosisAssociationCohort(
    CLAIMNUMBER: String,
    SEQUENCENUMBER: Option[String],
    DIAGNOSISCODE: Option[String],
    ICD10_OR_HIGHER: Boolean,
    CLAIMTYPE: Option[String],
    SITEID: Option[String],
    DIAGCODEFORMATTED: Option[String]
  )
}
