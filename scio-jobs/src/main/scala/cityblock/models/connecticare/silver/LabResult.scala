package cityblock.models.connecticare.silver

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object LabResult {
  @BigQueryType.toTable
  case class ParsedLabResults(
    LOB1: Option[String],
    NMI: String,
    MbrFirstName: Option[String],
    MbrLastName: Option[String],
    MbrBirthDate: Option[LocalDate],
    DateEff: Option[LocalDate],
    CPTCode: Option[String],
    TestCode: Option[String],
    TestName: Option[String],
    LabCode: Option[String],
    ResultCode: Option[String],
    ResultName: Option[String],
    ResultUnits: Option[String],
    ResultNum: Option[String],
    ResultNum_RangeLow: Option[String],
    ResultNum_RangeHigh: Option[String],
    ResultLiteral: Option[String],
    ResultLiteral_Range: Option[String],
    AbnormalFlag: Option[String],
    NationalOrderCode: Option[String],
    NationalResultCode: Option[String]
  )
}
