package cityblock.models.connecticare.silver

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object ProviderDetail {
  @BigQueryType.toTable
  case class ParsedProviderDetail(
    LOB: Option[String],
    ProvNum: String,
    ProvNPI: Option[String],
    ProvFirstName: Option[String],
    ProvLastName: Option[String],
    ProvTIN: Option[String],
    ProvTINName: Option[String],
    HATCd: Option[String],
    HATCd_Desc: Option[String],
    ProvClass: Option[String],
    ProvClass_Desc: Option[String],
    ProvCat: Option[String],
    ProvCat_Desc: Option[String],
    ProvSpec1: Option[String],
    ProvSpec1_Desc: Option[String],
    ProvStatus: Option[String],
    ProvStatus_Desc: Option[String],
    DateEnd: Option[LocalDate]
  )
}
