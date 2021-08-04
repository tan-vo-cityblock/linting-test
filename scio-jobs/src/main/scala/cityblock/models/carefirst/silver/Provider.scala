package cityblock.models.carefirst.silver

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object Provider {
  @BigQueryType.toTable
  case class ParsedProvider(
    PROV_ID: String,
    PROV_NPI: Option[String],
    PROV_TIN: Option[String],
    PROV_EFF_FROM_DATE: Option[LocalDate],
    PROV_EFF_TO_DATE: Option[LocalDate],
    PROV_TYPE: Option[String],
    PROV_TAXONOMY: Option[String],
    PROV_LIC: Option[String],
    PROV_MEDICAID: Option[String],
    PROV_DEA: Option[String],
    MEDICARE_CCN: Option[String],
    MCRB_NUM: Option[String],
    UPIN_NUM: Option[String],
    PROV_NET_FLAG: Option[String],
    PROV_PAR_FLAG: Option[String],
    PROV_LNAME: Option[String],
    PROV_FNAME: Option[String],
    PROV_MNAME: Option[String],
    PROV_GENDER: Option[String],
    PROV_DOB: Option[LocalDate],
    PROV_PHONE: Option[String],
    PROV_EMAIL: Option[String],
    PROV_LOC: Option[String],
    PROV_CLINIC_ID: Option[String],
    PROV_CLINIC_NAME: Option[String],
    PROV_CLINIC_ADDR: Option[String],
    PROV_CLINIC_ADDR2: Option[String],
    PROV_CLINIC_CITY: Option[String],
    PROV_CLINIC_STATE: Option[String],
    PROV_CLINIC_ZIP: Option[String],
    PROV_MAILING_ADDR: Option[String],
    PROV_MAILING_ADDR2: Option[String],
    PROV_MAILING_CITY: Option[String],
    PROV_MAILING_STATE: Option[String],
    PROV_MAILING_ZIP: Option[String],
    PROV_GRP_ID: Option[String],
    PROV_GRP_NAME: Option[String],
    PROV_GRP_MAILING_ADDR: Option[String],
    PROV_GRP_MAILING_ADDR2: Option[String],
    PROV_GRP_MAILING_CITY: Option[String],
    PROV_GRP_MAILING_STATE: Option[String],
    PROV_GRP_MAILING_ZIP: Option[String],
    PROV_GRP_PHONE: Option[String],
    PROV_GRP_EMAIL: Option[String],
    PROV_SPEC_CODE: Option[String],
    PROV_SPEC_DESC: Option[String],
    PROV_SPEC_CODE2: Option[String],
    PROV_SPEC_DESC2: Option[String],
    PROV_SPEC_CODE3: Option[String],
    PROV_SPEC_DESC3: Option[String],
    PROV_PCP_FLAG: Option[String],
    PROV_OB_FLAG: Option[String],
    PROV_MH_FLAG: Option[String],
    PROV_EYE_FLAG: Option[String],
    PROV_DEN_FLAG: Option[String],
    PROV_CD_FLAG: Option[String],
    PROV_NP_FLAG: Option[String],
    PROV_PA_FLAG: Option[String],
    PROV_RX_FLAG: Option[String],
    PROV_GROUP_IPA_NAME: Option[String],
    PROV_GROUP_ACO_NAME: Option[String],
    PROV_ATTRIB_FLAG: Option[String]
  )
}
