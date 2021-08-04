package cityblock.utilities

import java.util.concurrent.atomic.AtomicInteger

import cityblock.member.service.models.PatientInfo.{Datasource, Patient}
import cityblock.models.ConnecticareSilverClaims._
import cityblock.models.SilverIdentifier
import cityblock.models.connecticare.silver.LabResult.ParsedLabResults
import cityblock.models.connecticare.silver.Medical.ParsedMedical
import cityblock.models.connecticare.silver.MedicalDiagnosis.ParsedMedicalDiagnosis
import cityblock.models.connecticare.silver.MedicalICDProcedure.ParsedMedicalICDProcedure
import cityblock.models.connecticare.silver.Member.ParsedMember
import cityblock.models.connecticare.silver.Pharmacy.ParsedPharmacy
import cityblock.models.connecticare.silver.PharmacyMedicare.ParsedPharmacyMedicare
import cityblock.models.connecticare.silver.ProviderAddress.ParsedProviderAddress
import cityblock.models.connecticare.silver.ProviderDetail.ParsedProviderDetail
import cityblock.models.connecticare.silver.UBH.ParsedUBH
import cityblock.models.connecticare.silver.UBHDiagnosis.ParsedUBHDiagnosis
import cityblock.models.connecticare.silver.UBHMedicare.ParsedUBHMedicare
import cityblock.transforms.Transform
import org.joda.time.LocalDate

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap

/**
 * Default parsed CCI claims objects for use in tests. To use these objects in a test, call
 * * Default.[object].copy(...) and modify the desired fields.
 *
 * Note that values in Defaults do not share keys, the tester must manually specify
 * relationships.
 *
 * Eventually this object should expose a builder interface, but for now we expose
 * the default values directly.
 */
object CCISilverClaimsMocks {
  private val counters: TrieMap[String, AtomicInteger] = TrieMap()

  @tailrec
  private def id(name: String = "default"): String =
    counters.get(name) match {
      case Some(atomic) => s"$name(${atomic.addAndGet(1)})"
      case _ =>
        counters(name) = new AtomicInteger(0)
        id(name)
    }

  private val defaultDate = LocalDate.parse("2018-07-01")

  private def patient(externalId: String, patientId: Option[String] = None) =
    Patient(
      patientId = patientId,
      externalId = externalId,
      source = Datasource(name = "connecticare", commonId = None)
    )

  def member(
    NMI: String = id("member"),
    PCP_Num: Option[String] = None,
    ApplyMo: Option[LocalDate] = None,
    patientId: Option[String] = None
  ): Member =
    Member(
      identifier,
      patient(NMI, patientId),
      member.copy(NMI = NMI, PCP_Num = PCP_Num, ApplyMo = ApplyMo)
    )

  def providerDetail(
    ProvNum: String = id("providerDetail"),
    ProvNPI: Option[String] = Some(id("providerDetail.NPI"))
  ): ProviderDetail =
    ProviderDetail(
      identifier,
      providerDetail.copy(ProvNum = ProvNum, ProvNPI = ProvNPI)
    )

  def providerAddress(ProvNum: String = id("providerAddress")): ProviderAddress =
    ProviderAddress(
      identifier,
      providerAddress.copy(ProvNum = ProvNum)
    )

  def professional(
    ClmNum: String = id("professional.ClmNum"),
    ClmServNum: String = id("professional.ClmServNum"),
    NMI: String = id("professional.NMI"),
    ServProvTIN: Option[String] = None,
    RefProvNum: Option[String] = None,
    ServProvNum: Option[String] = None,
    patientId: Option[String] = None
  ): Medical =
    Medical(
      identifier,
      patient(NMI, patientId),
      medical.copy(
        ClmNum = ClmNum,
        ClmServNum = ClmServNum,
        NMI = NMI,
        ServProvTIN = ServProvTIN,
        RefProvNum = RefProvNum,
        ServProvNum = ServProvNum
      )
    )

  def facility(
    ClmNum: String = "facility.ClmNum",
    ClmServNum: String = id("facility.ClmServNum"),
    NMI: String = id("facility.NMI"),
    ServProvTIN: Option[String] = None,
    RefProvNum: Option[String] = None,
    ServProvNum: Option[String] = None,
    patientId: Option[String] = None,
    BillTypeCd: Option[String] = Some("facility.BillTypeCd"),
    Proc1: Option[String] = Some("REV Code"),
    Proc1Type: Option[String] = Some("REV")
  ): Medical =
    Medical(
      identifier,
      patient(NMI, patientId),
      medical.copy(
        ClmNum = ClmNum,
        ClmServNum = ClmServNum,
        NMI = NMI,
        ServProvTIN = ServProvTIN,
        RefProvNum = RefProvNum,
        ServProvNum = ServProvNum,
        BillTypeCd = BillTypeCd,
        Proc1 = Proc1,
        Proc1Type = Proc1Type
      )
    )

  def diagnosis(
    ClmNum: String,
    DiagCd: String = "ICD10Cm diagnosis code",
    DiagCategory: String = "Secondary"
  ): MedicalDiagnosis =
    MedicalDiagnosis(
      identifier,
      ParsedMedicalDiagnosis(
        ClmNum = ClmNum,
        DiagCategory = Some(DiagCategory),
        DiagCd = Some(DiagCd),
        ICD_Indicator = Some("I0") // this value comes straight from the claims
      )
    )

  def procedure(
    ClmNum: String,
    ICDProcCd: String = "ICD10Pcs procedure code",
    ICDProcCategory: String = "Secondary",
    ICD_Indicator: String = "I0"
  ): MedicalICDProcedure =
    MedicalICDProcedure(
      identifier,
      ParsedMedicalICDProcedure(
        ClmNum = ClmNum,
        ICDProcCategory = Some(ICDProcCategory),
        ICDProcCd = Some(ICDProcCd),
        ICD_Indicator = Some(ICD_Indicator)
      )
    )

  def labResult(
    NMI: String = id("labResult"),
    ResultLiteral: Option[String],
    CPTCode: Option[String] = None,
    TestName: Option[String] = None,
    ResultUnits: Option[String] = None,
    NationalResultCode: Option[String] = None,
    TestCode: Option[String] = None,
    patientId: Option[String] = None
  ): LabResults =
    LabResults(
      identifier,
      patient(NMI, patientId),
      labResult.copy(
        NMI = NMI,
        CPTCode = CPTCode,
        TestName = TestName,
        ResultLiteral = ResultLiteral,
        ResultUnits = ResultUnits,
        TestCode = TestCode,
        NationalResultCode = NationalResultCode
      )
    )

  def pharmacy(
    NMI: String,
    patientId: Option[String] = None,
    PrescribProvNum: Option[String] = None,
    PrescribPharmacyNum: Option[String] = None
  ): Pharmacy =
    Pharmacy(
      identifier,
      patient(NMI, patientId),
      pharmacy.copy(
        NMI = NMI,
        PrescribProvNum = PrescribProvNum,
        PrescribPharmacyNum = PrescribPharmacyNum
      )
    )

  def pharmacyMedicare(
    NMI: String,
    patientId: Option[String] = None,
    PrescribProvNum: Option[String] = None,
    PrescribPharmacyNum: Option[String] = None
  ): PharmacyMedicare =
    PharmacyMedicare(
      identifier,
      patient(NMI, patientId),
      pharmacyMedicare.copy(
        NMI = NMI,
        PrescribProvNum = PrescribProvNum,
        PrescribPharmacyNum = PrescribPharmacyNum
      )
    )

  def ubh(memberId: String, billCode: Option[String]): UBH =
    UBH(
      identifier,
      patient(memberId, None),
      ubh.copy(BILL = billCode)
    )

  def ubhMedicare(memberId: String, billCode: Option[String]): UBHMedicare =
    UBHMedicare(
      identifier,
      patient(memberId, None),
      ubhMedicare.copy(BILL = billCode)
    )

  def ubhDiagnosis: UBHDiagnosis =
    UBHDiagnosis(
      identifier,
      behavioralDiagnosis
    )

  private def identifier =
    SilverIdentifier(surrogateId = id("surrogate"), joinId = None)

  private val member =
    ParsedMember(
      Partnership_Current = None,
      ApplyMo = Some(defaultDate),
      LOB1 = Some("M"),
      LOB2 = Some("M"),
      NMI = "NMI",
      NMI_Sub = Some("NMI_Sub"),
      First_Name = Some("Elena"),
      Last_Name = Some("Kagan"),
      Gender = Some("F"),
      DateBirth = Some(LocalDate.parse("1960-04-28")),
      Age = Some("58"),
      PCP_Num = None,
      PCP_AffNum = None,
      PCP_NPI = None,
      PCP_TIN = None,
      ContractClass = None,
      GrpNum = None,
      DivNum = None,
      BenPack = None,
      Address1 = Some("1 First St NE"),
      Address2 = None,
      City = Some("Washington"),
      County = Some("Washington"),
      State = Some("District of Columbia"),
      Zip = Some("20543"),
      Phone1 = None
    )

  private val providerDetail =
    ParsedProviderDetail(
      LOB = Some("M"),
      ProvNum = "A",
      ProvNPI = Some("ProvNPI"),
      ProvFirstName = Some("Rebecca"),
      ProvLastName = Some("Crumpler"),
      ProvTIN = Some(Transform.ELIDED),
      ProvTINName = None,
      HATCd = None,
      HATCd_Desc = None,
      ProvClass = None,
      ProvClass_Desc = None,
      ProvCat = None,
      ProvCat_Desc = None,
      ProvSpec1 = None,
      ProvSpec1_Desc = None,
      ProvStatus = None,
      ProvStatus_Desc = None,
      DateEnd = None
    )

  private val providerAddress =
    ParsedProviderAddress(
      ProvNum = "B",
      Address1 = Some("1 Main St"),
      Address2 = None,
      ZipCd = Some("19702"),
      City = Some("Christiana"),
      State = Some("DE"),
      County = Some("New Castle")
    )

  private val medical =
    ParsedMedical(
      Partnership_Current = None,
      NMI = "Medical NMI",
      NMI_Sub = None,
      LOB1 = Some("M"),
      LOB2 = Some("M"),
      Age = None,
      Gender = None,
      GrpNum = None,
      DivNum = None,
      BenPack = None,
      COCLevel1 = Some("M"),
      COCLevel2 = Some("M"),
      ClmNum = "ClmNum",
      ClmServNum = "100",
      ClaimType = None,
      PCP_Num = None,
      PCP_AffNum = None,
      PCP_NPI = None,
      PCP_TIN = None,
      ServProvNum = None,
      ServProvAffNum = None,
      ServProvNPI = None,
      ServProvTIN = None,
      ServProvSpec1 = None,
      ServProvClass = None,
      ServProvCat = None,
      RefProvNum = None,
      RefProvAffNum = None,
      RefProvNPI = None,
      RefProvTIN = None,
      DateEff = Some(defaultDate),
      DateEnd = Some(defaultDate.plusDays(10)),
      DateAdmit = Some(defaultDate),
      DateDischarge = Some(defaultDate.plusDays(2)),
      DatePaid = Some(defaultDate.plusDays(20)),
      Proc1 = None,
      Proc1Type = None,
      Location = None,
      Modifer1 = Some("CPT modifier"),
      Modifier2 = Some("CPT modifier"),
      DRG = Some("DRG"),
      MDC = None,
      AdmitType = Some("Admit type code"),
      DischargeStatus = None,
      PaymentStatus_NetworkInOut = None,
      ERAvoid_Flag = Some("Y"),
      InptFac_Acute_SubAcute = None,
      Qty = None,
      Days = None,
      AmtCharge = Some("182"),
      AmtAllow = Some("3"),
      AmtCoins = Some("0"),
      AmtCopay = Some("0"),
      AmtDeduct = Some("0"),
      AmtPay = Some("56.21"),
      Explain1 = None,
      Explain2 = None,
      Proc2 = Some("HCPCS code"),
      Proc2Type = Some("HCPC"),
      BillTypeCd = None,
      CareCentrix = None
    )

  private val labResult = ParsedLabResults(
    LOB1 = Some("M"),
    NMI = "Lab Result NMI",
    MbrFirstName = Some("Firstly"),
    MbrLastName = Some("Lastly"),
    MbrBirthDate = Some(LocalDate.parse("1993-06-10")),
    DateEff = Some(LocalDate.parse("2018-10-18")),
    CPTCode = Some("88142"),
    TestCode = None,
    TestName = Some("ABSOLUTE EOS"),
    LabCode = None,
    ResultCode = None,
    ResultName = None,
    ResultUnits = None,
    ResultNum = None,
    ResultNum_RangeHigh = None,
    ResultNum_RangeLow = None,
    ResultLiteral = Some("0.3"),
    ResultLiteral_Range = None,
    AbnormalFlag = None,
    NationalOrderCode = None,
    NationalResultCode = None
  )

  private val pharmacy = ParsedPharmacy(
    Partnership_Current = None,
    NMI = "Pharmacy NMI",
    NMI_Sub = None,
    LOB1 = None,
    LOB2 = None,
    Age = None,
    Gender = None,
    GrpNum = None,
    DivNum = None,
    BenPack = None,
    ClmNum = "Pharmacy ClmNum",
    RxNum = None,
    AdjudNum = None,
    PCP_Num = None,
    PCP_AffNum = None,
    PCP_NPI = None,
    PCP_TIN = None,
    PrescribProvNum = None,
    PrescribProvNPI = None,
    PrescribProvTIN = None,
    PrescribDEANum = None,
    PrescribPharmacyNum = None,
    PrescribPharmacyNPI = None,
    PrescribPharmacyName = None,
    DateFill = None,
    DateBilled = None,
    DateTrans = None,
    RefillNum = None,
    NDC = None,
    DrugName = None,
    GPI = None,
    GCN = None,
    GBrand_Flag = None,
    GBSourceCd = None,
    TheraClassCd = None,
    Formulary_Flag = None,
    SpecialtyDrug_Flag = None,
    Tier = None,
    Retail_MailOrder_Flag = None,
    DAWCd = None,
    DaysSupply = None,
    PaidQty = None,
    ScriptCount1 = None,
    ScriptCount2 = None,
    AmtAWP = None,
    AmtGross = None,
    AmtTierCopay = None,
    AmtDeduct = None,
    AmtPaid = None
  )

  private val pharmacyMedicare = ParsedPharmacyMedicare(
    Partnership_Current = None,
    NMI = "Pharmacy NMI",
    NMI_Sub = None,
    LOB1 = None,
    LOB2 = None,
    Age = None,
    Gender = None,
    GrpNum = None,
    DivNum = None,
    BenPack = None,
    ClmNum = "Pharmacy ClmNum",
    RxNum = None,
    AdjudNum = None,
    PCP_Num = None,
    PCP_AffNum = None,
    PCP_NPI = None,
    PCP_TIN = None,
    PrescribProvNum = None,
    PrescribProvNPI = None,
    PrescribProvTIN = None,
    PrescribDEANum = None,
    PrescribPharmacyNum = None,
    PrescribPharmacyNPI = None,
    PrescribPharmacyName = None,
    DateFill = None,
    DateBilled = None,
    DateTrans = None,
    RefillNum = None,
    NDC = None,
    DrugName = None,
    GPI = None,
    GCN = None,
    GBrand_Flag = None,
    TheraClassCd = None,
    Formulary_Flag = None,
    SpecialtyDrug_Flag = None,
    Tier = None,
    Retail_MailOrder_Flag = None,
    DAWCd = None,
    DaysSupply = None,
    PaidQty = None,
    ScriptCount1 = None,
    ScriptCount2 = None,
    AmtAWP = None,
    AmtGross = None,
    AmtTierCopay = None,
    AmtDeduct = None,
    AmtPaid = None
  )

  private val ubh = ParsedUBH(
    AUDNBR_Header = "AUDNBR_Header",
    AUDNBR = "AUDNBR",
    Det_NBR = None,
    HMO_ID = None,
    MBR_ALT_ID = "MBR_ALT_ID",
    AGE = None,
    SEX = None,
    MBR_DOB = None,
    REL_CODE = None,
    GROUP = None,
    CLIENT = None,
    BPL = None,
    CONTR_PROV = None,
    PRV_SPEC = None,
    PRV_NBR = None,
    PRV_NM = None,
    PRV_FED_TAX_ID = None,
    BILL_PRV_NPI = None,
    SERV_PRV_NPI = None,
    PRV_ALT_ID = None,
    PRV_OFF_ADDR1 = None,
    PRV_OFF_ADDR2 = None,
    PRV_OFF_CITY = None,
    PRV_OFF_STATE = None,
    PRV_ZIP = None,
    Format = None,
    FROM_DOS = None,
    TO_DOS = None,
    ADMIT_DATE_STAGE = None,
    ICD_CDE_IND = None,
    PRIM_DX = None,
    PRIM_POA = None,
    PROC_MTHD = None,
    PROC_CD1 = None,
    PROC_MOD = None,
    CC_MOD = None,
    PROC_CD2 = None,
    REV_CD = None,
    DRG = None,
    BILL = None,
    ADMIT_TYPE = None,
    ADMIT_SRC = None,
    STATUS = None,
    POS = None,
    BEN_LVL = None,
    FINC_PRODUCT = None,
    LGL_ENTITY = None,
    SVC_CD = None,
    COPAY_NBR = None,
    COPAY_CALC_TYPE = None,
    COPAY_ACCUM_CD = None,
    DED_NBR = None,
    DED_ACCUM_CODE = None,
    ICD9_PROC_ONE = None,
    ICD9_PROC_TWO = None,
    ICD9_PROC_THREE = None,
    ICD9_PROC_FOUR = None,
    ICD9_PROC_FIVE = None,
    ICD9_PROC_SIX = None,
    TIERED_COPAY_FLAG = None,
    LTD_SERV_AFFECTED = None,
    LTD_SERV_NBR = None,
    LTD_SERV_TYPE = None,
    LTD_SERV_MULT = None,
    UNITS = None,
    DAYS = None,
    TOT_CLAIMED = None,
    CLM_AMT = None,
    ALLOW_AMT = None,
    DIS_AMT = None,
    DISC_AMT = None,
    COPAY_AMT = None,
    DED_AMT = None,
    RES_AMT = None,
    PAID_AMT = None,
    LOB = None,
    PAID = None
  )

  private val ubhMedicare = ParsedUBHMedicare(
    AUDNBR_Header = "AUDNBR_Header",
    AUDNBR = "AUDNBR",
    Det_NBR = None,
    HMO_ID = None,
    MBR_ALT_ID = "MBR_ALT_ID",
    AGE = None,
    SEX = None,
    MBR_DOB = None,
    REL_CODE = None,
    GROUP = None,
    CLIENT = None,
    BPL = None,
    CONTR_PROV = None,
    PRV_SPEC = None,
    PRV_NBR = None,
    PRV_NM = None,
    PRV_FED_TAX_ID = None,
    BILL_PRV_NPI = None,
    SERV_PRV_NPI = None,
    PRV_ALT_ID = None,
    PRV_OFF_ADDR1 = None,
    PRV_OFF_ADDR2 = None,
    PRV_OFF_CITY = None,
    PRV_OFF_STATE = None,
    PRV_ZIP = None,
    Format = None,
    FROM_DOS = None,
    TO_DOS = None,
    ADMIT_DATE_STAGE = None,
    ICD_CDE_IND = None,
    PRIM_DX = None,
    PRIM_POA = None,
    PROC_MTHD = None,
    PROC_CD1 = None,
    PROC_MOD = None,
    CC_MOD = None,
    PROC_CD2 = None,
    REV_CD = None,
    DRG = None,
    BILL = None,
    ADMIT_TYPE = None,
    ADMIT_SRC = None,
    STATUS = None,
    POS = None,
    BEN_LVL = None,
    FINC_PRODUCT = None,
    LGL_ENTITY = None,
    SVC_CD = None,
    COPAY_NBR = None,
    COPAY_CALC_TYPE = None,
    COPAY_ACCUM_CD = None,
    DED_NBR = None,
    DED_ACCUM_CODE = None,
    ICD9_PROC_ONE = None,
    ICD9_PROC_TWO = None,
    ICD9_PROC_THREE = None,
    ICD9_PROC_FOUR = None,
    ICD9_PROC_FIVE = None,
    ICD9_PROC_SIX = None,
    TIERED_COPAY_FLAG = None,
    LTD_SERV_AFFECTED = None,
    LTD_SERV_NBR = None,
    LTD_SERV_TYPE = None,
    LTD_SERV_MULT = None,
    UNITS = None,
    DAYS = None,
    TOT_CLAIMED = None,
    CLM_AMT = None,
    ALLOW_AMT = None,
    DIS_AMT = None,
    DISC_AMT = None,
    COPAY_AMT = None,
    DED_AMT = None,
    RES_AMT = None,
    PAID_AMT = None,
    NMI_Legacy = None,
    LOB = None,
    PAID = None
  )

  private val behavioralDiagnosis = ParsedUBHDiagnosis(
    AUDNBR_Header = "AUDNBR_Header",
    DiagnosisIdNum = "DiagnosisIdNum",
    DiagnosisNbr = 1,
    DiagnosisTypeDesc = None,
    ICDVersion = None,
    LOB = None
  )
}
