package cityblock.models.connecticare.silver

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object Member {
  @BigQueryType.toTable
  case class ParsedMember(
    Partnership_Current: Option[String],
    ApplyMo: Option[LocalDate],
    LOB1: Option[String],
    LOB2: Option[String],
    NMI: String,
    NMI_Sub: Option[String],
    First_Name: Option[String],
    Last_Name: Option[String],
    Gender: Option[String],
    DateBirth: Option[LocalDate],
    Age: Option[String],
    PCP_Num: Option[String],
    PCP_AffNum: Option[String],
    PCP_NPI: Option[String],
    PCP_TIN: Option[String],
    ContractClass: Option[String],
    GrpNum: Option[String],
    DivNum: Option[String],
    BenPack: Option[String],
    Address1: Option[String],
    Address2: Option[String],
    City: Option[String],
    County: Option[String],
    State: Option[String],
    Zip: Option[String],
    Phone1: Option[String]
  )
}
