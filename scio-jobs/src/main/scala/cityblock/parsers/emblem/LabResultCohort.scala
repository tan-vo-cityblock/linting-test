package cityblock.parsers.emblem

import com.spotify.scio.bigquery.types.BigQueryType

object LabResultCohort {

  @BigQueryType.toTable
  case class ParsedLabResultCohort(
    SITEID: String,
    LABRESULTSID: String,
    MEMBERID: String,
    BENEFITPLANID: String,
    BENEFITSEQNUMBER: String,
    EMPLOYERGROUPID: String,
    EMPLOYERGROUPMODIFIER: String,
    MBRFIRSTNAME: String,
    MBRLASTNAME: String,
    MEMBERBIRTHDATE: Option[String],
    LABDATE: Option[String], // only difference from LabResult.ParsedLabResult
    DIAGCODE: Option[String],
    PROCCODE1: Option[String],
    PROCCODE2: Option[String],
    LOCALORDERCODE: Option[String],
    LOCALTESTNAME: Option[String],
    LOCALTESTRESULT: Option[String],
    LOCALTESTUNITS: Option[String],
    NATLORDERCODE: Option[String],
    NATLRESULTCODE: Option[String],
    ICDVERSION: Option[String],
    PROCESSINGENTITY: Option[String]
  )
}
