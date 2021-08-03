package cityblock.transforms.connecticare

import cityblock.models.ConnecticareSilverClaims._
import cityblock.models.Surrogate
import cityblock.models.connecticare.silver.UBH.ParsedUBH
import cityblock.models.gold.Claims.{Diagnosis, Procedure}
import cityblock.transforms.Transform
import cityblock.transforms.Transform._
import cityblock.transforms.connecticare.gold.FacilityTransformer.procCodesAndTypes
import cityblock.utilities.{Conversions, PartnerConfiguration}
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

package object gold {
  val partner: String = PartnerConfiguration.connecticare.indexName

  type IndexedDiagnoses = (ClaimKey, Iterable[Diagnosis])
  type IndexedProcedures = (ClaimKey, Iterable[Procedure])
  type NMI = String
  type PatientId = String

  val IndicatorIsICD10: String = "I0"

  sealed case class ClaimKey(num: String) extends Key
  object ClaimKey {
    def apply(medical: Medical): ClaimKey = ClaimKey(medical.medical.ClmNum)
    def apply(diagnosis: MedicalDiagnosis): ClaimKey =
      ClaimKey(diagnosis.diagnosis.ClmNum)
    def apply(procedure: MedicalICDProcedure): ClaimKey =
      ClaimKey(procedure.procedure.ClmNum)
    def apply(behavioral: UBH): ClaimKey =
      ClaimKey(behavioral.claim.AUDNBR)
    def apply(behavioral: UBHMedicare): ClaimKey = ClaimKey(behavioral.claim.AUDNBR)
  }

  def index[S: Coder, K: Coder, G: Coder](
    project: String,
    dataset: String,
    table: String,
    items: SCollection[S],
    surrogateIdFn: S => String,
    keyFn: S => K,
    transformFn: (Surrogate, S) => Option[G]
  ): SCollection[(K, Iterable[G])] =
    items
      .withName("Add surrogates")
      .map {
        addSurrogate(project, dataset, table, _)(surrogateIdFn)
      }
      .withName("Construct index")
      .flatMap {
        case (surrogate, value) =>
          for (transformed <- transformFn(surrogate, value))
            yield (keyFn(value), transformed)
      }
      .withName("Group by claim key")
      .groupByKey
      .filter {
        case (_, itemsGrouped) => itemsGrouped.nonEmpty
      }

  def isCapitated(COCLevel2: Option[String]): Option[Boolean] =
    COCLevel2 match {
      case None              => None
      case Some("Capitated") => Some(true)
      case _                 => Some(false)
    }

  object ProcType extends Enumeration {
    val HCPC, CPT, REV, OTHER = Value

    def fromString(s: String): Option[Value] = values.find(_.toString == s)
    def toCodeSet(v: Value): Option[Transform.CodeSet.Value] = v match {
      case HCPC => Some(Transform.CodeSet.HCPCS)
      case CPT  => Some(Transform.CodeSet.CPT)
      case _    => None
    }
  }

  def revenueCode(silver: Medical): Option[String] =
    procCodesAndTypes(silver)
      .flatMap({
        case (proc, procType) if procType.contains(ProcType.REV.toString) => proc
        case _                                                            => None
      })
      .headOption

  def isFacility(silverClaims: Iterable[Medical]): Boolean =
    silverClaims.exists(silver => {
      revenueCode(silver).isDefined || silver.medical.BillTypeCd.isDefined
    })

  def isProfessional(silverClaims: Iterable[Medical]): Boolean =
    !isFacility(silverClaims)

  def isFacilityUBH(claims: Iterable[UBH]): Boolean =
    claims.exists(claim => {
      claim.claim.BILL.isDefined || (claim.claim.REV_CD.isDefined && claim.claim.REV_CD
        .exists(code => code != "0000"))
    })

  def isFacilityUBHMedicare(claims: Iterable[UBHMedicare]): Boolean =
    claims.exists(claim => {
      claim.claim.BILL.isDefined || (claim.claim.REV_CD.isDefined && claim.claim.REV_CD
        .exists(code => code != "0000"))
    })

  def isProfessionalUBH(claims: Iterable[UBH]): Boolean = !isFacilityUBH(claims)

  def isProfessionalUBHMedicare(claims: Iterable[UBHMedicare]): Boolean =
    !isFacilityUBHMedicare(claims)

  def toCommercial(medicare: UBHMedicare): UBH = {
    val claim = medicare.claim
    UBH(
      identifier = medicare.identifier,
      patient = medicare.patient,
      claim = ParsedUBH(
        AUDNBR_Header = claim.AUDNBR_Header,
        AUDNBR = claim.AUDNBR,
        Det_NBR = claim.Det_NBR,
        HMO_ID = claim.HMO_ID,
        MBR_ALT_ID = claim.MBR_ALT_ID,
        AGE = claim.AGE,
        SEX = claim.SEX,
        MBR_DOB = claim.MBR_DOB,
        REL_CODE = claim.REL_CODE,
        GROUP = claim.GROUP,
        CLIENT = claim.CLIENT,
        BPL = claim.BPL,
        CONTR_PROV = claim.CONTR_PROV,
        PRV_SPEC = claim.PRV_SPEC,
        PRV_NBR = claim.PRV_NBR,
        PRV_NM = claim.PRV_NM,
        PRV_FED_TAX_ID = claim.PRV_FED_TAX_ID,
        BILL_PRV_NPI = claim.BILL_PRV_NPI,
        SERV_PRV_NPI = claim.SERV_PRV_NPI,
        PRV_ALT_ID = claim.PRV_ALT_ID,
        PRV_OFF_ADDR1 = claim.PRV_OFF_ADDR1,
        PRV_OFF_ADDR2 = claim.PRV_OFF_ADDR2,
        PRV_OFF_CITY = claim.PRV_OFF_CITY,
        PRV_OFF_STATE = claim.PRV_OFF_STATE,
        PRV_ZIP = claim.PRV_ZIP,
        Format = claim.Format,
        FROM_DOS = claim.FROM_DOS,
        TO_DOS = claim.TO_DOS,
        ADMIT_DATE_STAGE = claim.ADMIT_DATE_STAGE,
        ICD_CDE_IND = claim.ICD_CDE_IND,
        PRIM_DX = claim.PRIM_DX,
        PRIM_POA = claim.PRIM_POA,
        PROC_MTHD = claim.PROC_MTHD,
        PROC_CD1 = claim.PROC_CD1,
        PROC_MOD = claim.PROC_MOD,
        CC_MOD = claim.CC_MOD,
        PROC_CD2 = claim.PROC_CD2,
        REV_CD = claim.REV_CD,
        DRG = claim.DRG,
        BILL = claim.BILL,
        ADMIT_TYPE = claim.ADMIT_TYPE,
        ADMIT_SRC = claim.ADMIT_SRC,
        STATUS = claim.STATUS,
        POS = claim.POS,
        BEN_LVL = claim.BEN_LVL,
        FINC_PRODUCT = claim.FINC_PRODUCT,
        LGL_ENTITY = claim.LGL_ENTITY,
        SVC_CD = claim.SVC_CD,
        COPAY_NBR = claim.COPAY_NBR,
        COPAY_CALC_TYPE = claim.COPAY_CALC_TYPE,
        COPAY_ACCUM_CD = claim.COPAY_ACCUM_CD,
        DED_NBR = claim.DED_NBR,
        DED_ACCUM_CODE = claim.DED_ACCUM_CODE,
        ICD9_PROC_ONE = claim.ICD9_PROC_ONE,
        ICD9_PROC_TWO = claim.ICD9_PROC_TWO,
        ICD9_PROC_THREE = claim.ICD9_PROC_THREE,
        ICD9_PROC_FOUR = claim.ICD9_PROC_FOUR,
        ICD9_PROC_FIVE = claim.ICD9_PROC_FIVE,
        ICD9_PROC_SIX = claim.ICD9_PROC_SIX,
        TIERED_COPAY_FLAG = claim.TIERED_COPAY_FLAG,
        LTD_SERV_AFFECTED = claim.LTD_SERV_AFFECTED,
        LTD_SERV_NBR = claim.LTD_SERV_NBR,
        LTD_SERV_TYPE = claim.LTD_SERV_TYPE,
        LTD_SERV_MULT = claim.LTD_SERV_MULT,
        UNITS = claim.UNITS,
        DAYS = claim.DAYS,
        TOT_CLAIMED = claim.TOT_CLAIMED,
        CLM_AMT = claim.CLM_AMT,
        ALLOW_AMT = claim.ALLOW_AMT,
        DIS_AMT = claim.DIS_AMT,
        DISC_AMT = claim.DISC_AMT,
        COPAY_AMT = claim.COPAY_AMT,
        DED_AMT = claim.DED_AMT,
        RES_AMT = claim.RES_AMT,
        PAID_AMT = claim.PAID_AMT,
        LOB = claim.LOB,
        PAID = claim.PAID
      )
    )
  }

  val FacetsMedicalClaimLineNumberLength: Int = 2

  def mkFacetsPartnerClaimId(silver: FacetsMedical): String =
    silver.data.EXT_1001_CLAIM_NUMBER
      .substring(0, silver.data.EXT_1001_CLAIM_NUMBER.length - FacetsMedicalClaimLineNumberLength)

  def parseAndSumAmounts(amount: List[String]): BigDecimal =
    amount.flatMap(Conversions.safeParse(_, BigDecimal(_))).sum
}
