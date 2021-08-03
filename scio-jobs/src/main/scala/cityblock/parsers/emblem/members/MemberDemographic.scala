package cityblock.parsers.emblem.members

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object MemberDemographic {

  @BigQueryType.toTable
  case class ParsedMemberDemographic(
    MEM_START_DATE: Option[LocalDate],
    MEM_END_DATE: Option[LocalDate],
    MEMBER_ID: String,
    MEMBER_QUAL: Option[String],
    PERSON_ID: Option[String],
    MEM_GENDER: Option[String],
    MEM_SSN: Option[String],
    MEM_LNAME: Option[String],
    MEM_FNAME: Option[String],
    MEM_MNAME: Option[String],
    MEM_DOB: Option[LocalDate],
    MEM_DOD: Option[LocalDate],
    MEM_MEDICARE: Option[String],
    MEM_ADDR1: Option[String],
    MEM_ADDR2: Option[String],
    MEM_CITY: Option[String],
    COUNTY: Option[String],
    MEM_STATE: Option[String],
    MEM_ZIP: Option[String],
    MEM_EMAIL: Option[String],
    MEM_PHONE: Option[String],
    MEM_RACE: Option[String],
    MEM_ETHNICITY: Option[String],
    MEM_LANGUAGE: Option[String],
    MEM_DATA_SRC: Option[String],
    HIRE_DATE: Option[String],
    NMI: String,
    MARITALSTATUS: Option[String],
    SERVICEYEARMONTH: Option[String]
  )
}
