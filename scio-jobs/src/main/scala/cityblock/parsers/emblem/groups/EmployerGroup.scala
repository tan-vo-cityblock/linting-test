package cityblock.parsers.emblem.groups

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object EmployerGroup {

  @BigQueryType.toTable
  case class ParsedEmployerGroup(
    GRP_START_DATE: Option[String],
    GRP_END_DATE: Option[String],
    GRP_ID: Option[String],
    GRP_NAME: Option[String],
    GRP_SIC: Option[String],
    GRP_NAICS: Option[String],
    GRP_TYPE: Option[String],
    GRP_RENEW_MTH: Option[String],
    GRP_EFF_DATE: Option[LocalDate],
    GRP_TERM_DATE: Option[LocalDate],
    GRP_BROKER_ID: Option[String],
    GRP_ELIG_LAG: Option[String],
    GRP_CONTACT: Option[String],
    GRP_ADDR1: Option[String],
    GRP_ADDR2: Option[String],
    GRP_CITY: Option[String],
    GRP_STATE: Option[String],
    GRP_ZIP: Option[String],
    GRP_PHONE: Option[String],
    GRP_LVL_01: Option[String],
    GRP_LVL_02: Option[String],
    GRP_LVL_03: Option[String],
    GRP_DATA_SRC: Option[String],
    TYPEOFBUSINESS: Option[String],
    GROUPCOUNTY: Option[String],
    EMPLOYERGROUPMODIFIER: Option[String],
    DIVISIONID: Option[String],
    TOTALEMPLOYEE: Option[String],
    EMPLOYERTIN: Option[String],
    MARKETSEGMENTGROUPTYPECODE: Option[String]
  )
}
