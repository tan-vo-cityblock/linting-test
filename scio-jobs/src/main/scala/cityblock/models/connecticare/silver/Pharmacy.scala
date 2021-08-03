package cityblock.models.connecticare.silver

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object Pharmacy {
  @BigQueryType.toTable
  case class ParsedPharmacy(
    Partnership_Current: Option[String],
    NMI: String,
    NMI_Sub: Option[String],
    LOB1: Option[String],
    LOB2: Option[String],
    Age: Option[String],
    Gender: Option[String],
    GrpNum: Option[String],
    DivNum: Option[String],
    BenPack: Option[String],
    ClmNum: String,
    RxNum: Option[String],
    AdjudNum: Option[String],
    PCP_Num: Option[String],
    PCP_AffNum: Option[String],
    PCP_NPI: Option[String],
    PCP_TIN: Option[String],
    PrescribProvNum: Option[String],
    PrescribProvNPI: Option[String],
    PrescribProvTIN: Option[String],
    PrescribDEANum: Option[String],
    PrescribPharmacyNum: Option[String],
    PrescribPharmacyNPI: Option[String],
    PrescribPharmacyName: Option[String],
    DateFill: Option[LocalDate],
    DateBilled: Option[LocalDate],
    DateTrans: Option[LocalDate],
    RefillNum: Option[String],
    NDC: Option[String],
    DrugName: Option[String],
    GPI: Option[String],
    GCN: Option[String],
    GBrand_Flag: Option[String],
    GBSourceCd: Option[String],
    TheraClassCd: Option[String],
    Formulary_Flag: Option[String],
    SpecialtyDrug_Flag: Option[String],
    Tier: Option[String],
    Retail_MailOrder_Flag: Option[String],
    DAWCd: Option[String],
    DaysSupply: Option[String],
    PaidQty: Option[String],
    ScriptCount1: Option[String],
    ScriptCount2: Option[String],
    AmtAWP: Option[String],
    AmtGross: Option[String],
    AmtTierCopay: Option[String],
    AmtDeduct: Option[String],
    AmtPaid: Option[String]
  )
}
