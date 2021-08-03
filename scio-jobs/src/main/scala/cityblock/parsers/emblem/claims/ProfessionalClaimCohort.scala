package cityblock.parsers.emblem.claims

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object ProfessionalClaimCohort {

  @BigQueryType.toTable
  case class ParsedProfessionalClaimCohort(
    CLAIM_ID: String,
    CLAIM_SFX_OR_PARENT: Option[String],
    SV_LINE: Option[Int],
    FORM_TYPE: Option[String],
    SV_STAT: Option[String],
    ADM_DATE: Option[LocalDate],
    ADM_TYPE: Option[String],
    ADM_SRC: Option[String],
    DIS_DATE: Option[LocalDate],
    CLIENT_LOS: Option[String],
    FROM_DATE: Option[LocalDate],
    TO_DATE: Option[LocalDate],
    HOSP_TYPE: Option[String],
    MEMBER_ID: String,
    MEMBER_QUAL: Option[String],
    RELATION: Option[String],
    GRP_ID: Option[String],
    CLAIM_REC_DATE: Option[LocalDate],
    CLAIM_ENTRY_DATE: Option[LocalDate],
    PAID_DATE: Option[LocalDate],
    CHK_NUM: Option[String],
    MS_DRG: Option[String],
    TRI_DRG: Option[String],
    AP_DRG: Option[String],
    APR_DRG: Option[String],
    APR_DRG_SEV: Option[String],
    CONT_STAY_ID: Option[String],
    ICD_DIAG_ADMIT: Option[String],
    ICD_DIAG_01: Option[String],
    ICD_PROC_01: Option[String],
    ICD10_OR_HIGHER: Option[Boolean],
    ATT_PROV: Option[String],
    ATT_PROV_SPEC: Option[String],
    ATT_IPA: Option[String],
    ATT_ACO: Option[String],
    BILL_PROV: Option[String],
    REF_PROV: Option[String],
    ACO_ID: Option[String],
    MED_HOME_ID: Option[String],
    CONTRACT: Option[String],
    BEN_PKG_ID: Option[String],
    POS: Option[String],
    UB_BILL_TYPE: Option[String],
    TOS: Option[String],
    PROC_CODE: Option[String],
    CPT_MOD_1: Option[String],
    CPT_MOD_2: Option[String],
    REV_CODE: Option[String],
    NDC: Option[String],
    RX_DAYS_SUPPLY: Option[String],
    RX_QTY_DISPENSED: Option[String],
    RX_DRUG_COST: Option[String],
    RX_INGR_COST: Option[String],
    RX_DISP_FEE: Option[String],
    RX_DISCOUNT: Option[String],
    RX_DAW: Option[String],
    RX_FILL_SRC: Option[String],
    RX_REFILLS: Option[String],
    RX_PAR: Option[String],
    RX_FORM: Option[String],
    SV_UNITS: Option[Int],
    AMT_BILLED: Option[String],
    AMT_ALLOWED: Option[String],
    AMT_ALLOWED_STAT: Option[String],
    AMT_PAID: Option[String],
    AMT_HRA: Option[String],
    AMT_DEDUCT: Option[String],
    AMT_COINS: Option[String],
    AMT_COPAY: Option[String],
    AMT_COB: Option[String],
    AMT_WITHHOLD: Option[String],
    AMT_DISALLOWED: Option[String],
    AMT_MAXIMUM: Option[String],
    AUTH_ID: Option[String],
    DIS_STAT: Option[String],
    BABY_WGHT: Option[String],
    WEEKS_GEST: Option[String],
    BABY_CLAIM_ID: Option[String],
    CL_DATA_SRC: Option[String],
    MI_POST_DATE: Option[LocalDate],
    CLAIM_IN_NETWORK: Option[Boolean],
    MAINTENANCEDATE: Option[LocalDate],
    RESPONSIBLEINDICATOR: Option[String],
    OPID: Option[String],
    FFSCAPIND: Option[String],
    PROCESSINGENTITY: Option[String],
    SITEID: Option[String],
    CONTRACTORIGIN: Option[String],
    CARECORERESPONSIBLEAMT: Option[String],
    NYHCRASURCHARGEAMOUNT: Option[String],
    MEDICARESEQUESTRATIONIND: Option[String],
    MEDICAREORIGINALAMOUNT: Option[String],
    NATIVEALLOWEDAMOUNT: Option[String],
    PARTICIPATINGPROVIDERIND: Option[String],
    COBINDICATOR: Option[String],
    EMPLOYERGROUPMOD: Option[String],
    EMPLOYERGROUPTYPE: Option[String],
    BENEFITSEQUENCE: Option[String],
    EOCCODE: Option[String],
    SERVICINGPROVLOCSUFFIX: Option[String],
    BILLINGPROVLOCATIONSUFFIX: Option[String],
    REFERRINGPROVLOCATIONSUFFIX: Option[String],
    SERVICEYEARMONTH: Option[String],
    SERVICEACTIVITYDATE: Option[LocalDate],
    CHECKACCOUNTNUMBER: Option[String],
    CHECKDATE: Option[String],
    CLAIMFILE: Option[String],
    INTERESTAMOUNT: Option[String],
    LINEOFBUSINESS: Option[String],
    MEDICALCENTERNUMBER: Option[String],
    MEDICALGROUPID: Option[String],
    MEDICALREGION: Option[String],
    OTHERINSURANCEINDICATOR: Option[String],
    REMITTNUMBER: Option[String],
    RESPONSIBLEREASON: Option[String],
    SUPERGROUPID: Option[String],
    DIAGNOSISPOINTER1: Option[String],
    DIAGNOSISPOINTER2: Option[String],
    DIAGNOSISPOINTER3: Option[String],
    DIAGNOSISPOINTER4: Option[String],
    DIAGNOSISPOINTER5: Option[String],
    DIAGNOSISPOINTER6: Option[String],
    DIAGNOSISPOINTER7: Option[String],
    DIAGNOSISPOINTER8: Option[String],
    DIAGNOSISCODE1: Option[String],
    DIAGNOSISCODE2: Option[String],
    DIAGNOSISCODE3: Option[String],
    DIAGNOSISCODE4: Option[String],
    DIAGNOSISCODE5: Option[String],
    DIAGNOSISCODE6: Option[String],
    DIAGNOSISCODE7: Option[String],
    DIAGNOSISCODE8: Option[String],
    ICDREVISIONCODE: Option[String],
    PAYMENTSTATUS: Option[String]
  )
}