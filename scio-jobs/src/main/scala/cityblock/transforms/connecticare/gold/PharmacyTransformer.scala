package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.{Pharmacy, PharmacyMedicare}
import cityblock.models.connecticare.silver.Pharmacy.ParsedPharmacy
import cityblock.models.gold.Claims.{ClaimLineStatus, MemberIdentifier}
import cityblock.models.gold.Constructors._
import cityblock.models.gold.PharmacyClaim.{
  BrandIndicator,
  Date,
  DispenseMethod,
  Drug,
  DrugClass,
  DrugClassCodeset,
  PharmacyProvider,
  PrescribingProvider,
  Pharmacy => GoldPharmacy
}
import cityblock.models.gold.{Amount, Constructors, Helpers}
import cityblock.models.{Identifier, Surrogate}
import cityblock.transforms.Transform
import cityblock.transforms.Transform.Transformer
import cityblock.utilities.connecticare.Insurance.PharmacyMapping
import com.spotify.scio.values.SCollection
import cityblock.utilities.Conversions

case class PharmacyTransformer(
  commercial: SCollection[Pharmacy]
) extends Transformer[GoldPharmacy] {
  def transform()(implicit pr: String): SCollection[GoldPharmacy] =
    PharmacyTransformer.transform(pr, "Pharmacy", commercial)
}

object PharmacyTransformer {
  private[gold] def transform(
    project: String,
    table: String,
    claims: SCollection[Pharmacy]
  ): SCollection[GoldPharmacy] =
    claims
      .groupBy(PharmacyKey(_))
      .values
      .map(claims => mkAmountForClaim(claims.toList))
      .map {
        case (line, amount) =>
          val (surrogate, _) =
            Transform.addSurrogate(project, "silver_claims", table, line)(_.identifier.surrogateId)
          mkPharmacy(line, surrogate, amount)
      }

  private def mkAmountForClaim(linesByClaim: List[Pharmacy]): (Pharmacy, Amount) = {
    // NOTE: CCI sends multiple lines with the same claim ID, so we sum amounts over the lines and pick the most
    // recently paid claim line to fill in values. Most columns have the same value over lines except for the billing
    // date.
    val mostRecent: Pharmacy =
      linesByClaim.maxBy(_.pharmacy.DateBilled)(Transform.NoneMaxOptionLocalDateOrdering)

    val amounts: List[Amount] = linesByClaim.map(claim => {
      val data = claim.pharmacy
      Constructors.mkAmount(
        data.AmtGross,
        data.AmtAWP,
        None,
        data.AmtTierCopay,
        data.AmtDeduct,
        None,
        data.AmtPaid
      )
    })

    (mostRecent, Helpers.sumAmounts(amounts))
  }

  private[gold] def mkPharmacy(
    silver: Pharmacy,
    surrogate: Surrogate,
    amount: Amount
  ): GoldPharmacy =
    GoldPharmacy(
      identifier = Identifier(
        id = PharmacyKey(silver).uuid.toString,
        partner = partner,
        surrogate = surrogate
      ),
      memberIdentifier = MemberIdentifier(
        patientId = silver.patient.patientId,
        partnerMemberId = silver.patient.externalId,
        commonId = silver.patient.source.commonId,
        partner = partner
      ),
      capitatedFlag = None,
      claimLineStatus = ClaimLineStatus.Paid.toString,
      cobFlag = None,
      lineOfBusiness = PharmacyMapping.getLineOfBusiness(silver).map(_.toString),
      subLineOfBusiness = PharmacyMapping.getSubLineOfBusiness(silver).map(_.toString),
      pharmacy = mkPharmacyProvider(silver),
      prescriber = mkPrescribingProvider(silver),
      diagnosis = None,
      drug = mkDrug(silver),
      amount = amount,
      date = mkDate(silver)
    )

  private[gold] def mkPharmacyProvider(silver: Pharmacy): Option[PharmacyProvider] =
    for (num <- silver.pharmacy.PrescribPharmacyNum)
      yield
        PharmacyProvider(
          id = Some(ProviderKey(num).uuid.toString),
          npi = silver.pharmacy.PrescribPharmacyNPI,
          ncpdp = None,
          inNetworkFlag = None
        )

  private[gold] def mkPrescribingProvider(silver: Pharmacy): Option[PrescribingProvider] =
    for (num <- silver.pharmacy.PrescribProvNum)
      yield
        PrescribingProvider(
          id = ProviderKey(num).uuid.toString,
          npi = silver.pharmacy.PrescribProvNPI,
          specialty = None,
          placeOfService = None
        )

  private[gold] def mkDrug(silver: Pharmacy): Drug =
    Drug(
      ndc = silver.pharmacy.NDC,
      quantityDispensed = silver.pharmacy.PaidQty.flatMap(Conversions.safeParse(_, BigDecimal(_))),
      daysSupply = silver.pharmacy.DaysSupply.flatMap(Conversions.safeParse(_, _.toInt)),
      partnerPrescriptionNumber = silver.pharmacy.RxNum,
      fillNumber = silver.pharmacy.RefillNum.flatMap(Conversions.safeParse(_, _.toInt)),
      brandIndicator = brandIndicator(silver).toString,
      ingredient = None,
      strength = None,
      dispenseAsWritten = dispenseAsWritten(silver),
      dispenseMethod = dispenseMethod(silver).toString,
      classes = classes(silver),
      formularyFlag = silver.pharmacy.Formulary_Flag.contains("Y")
    )

  private[gold] def mkDate(silver: Pharmacy): Date =
    Date(filled = silver.pharmacy.DateFill, paid = None, billed = silver.pharmacy.DateBilled)

  private[gold] def brandIndicator(silver: Pharmacy): BrandIndicator.Value =
    silver.pharmacy.GBrand_Flag match {
      case Some("B") => BrandIndicator.Brand
      case Some("G") => BrandIndicator.Generic
      case _         => BrandIndicator.Unknown
    }

  private[gold] def dispenseAsWritten(silver: Pharmacy): Option[String] = {
    val digit = "[0-9]".r
    silver.pharmacy.DAWCd match {
      case None => None
      case Some(daw) =>
        daw match {
          case digit() => Some(daw)
          case _       => None
        }
    }
  }

  private[gold] def dispenseMethod(silver: Pharmacy): DispenseMethod.Value =
    silver.pharmacy.Retail_MailOrder_Flag match {
      case Some("Retail")    => DispenseMethod.Retail
      case Some("MailOrder") => DispenseMethod.Mail
      case _                 => DispenseMethod.Unknown
    }

  private[gold] def classes(silver: Pharmacy): List[DrugClass] =
    List(for (gpi <- silver.pharmacy.GPI)
           yield mkDrugClass(gpi, DrugClassCodeset.GPI),
         for (gcn <- silver.pharmacy.GCN)
           yield mkDrugClass(gcn, DrugClassCodeset.GCN)).flatten

  /**
   * Map silver [[PharmacyMedicare]] to silver [[Pharmacy]], setting to [[None]] all
   * fields that are exclusive to [[Pharmacy]] ([[Pharmacy]] is a superset of
   * [[PharmacyMedicare]]).
   */
  private[gold] def toCommercial(silver: PharmacyMedicare): Pharmacy = {
    val p = silver.pharmacy
    Pharmacy(
      identifier = silver.identifier,
      patient = silver.patient,
      pharmacy = ParsedPharmacy(
        Partnership_Current = p.Partnership_Current,
        NMI = p.NMI,
        NMI_Sub = p.NMI_Sub,
        LOB1 = p.LOB1,
        LOB2 = p.LOB2,
        Age = p.Age,
        Gender = p.Gender,
        GrpNum = p.GrpNum,
        DivNum = p.DivNum,
        BenPack = p.BenPack,
        ClmNum = p.ClmNum,
        RxNum = p.RxNum,
        AdjudNum = p.AdjudNum,
        PCP_Num = p.PCP_Num,
        PCP_AffNum = p.PCP_AffNum,
        PCP_NPI = p.PCP_NPI,
        PCP_TIN = p.PCP_TIN,
        PrescribProvNum = p.PrescribProvNum,
        PrescribProvNPI = p.PrescribProvNPI,
        PrescribProvTIN = p.PrescribProvTIN,
        PrescribDEANum = p.PrescribDEANum,
        PrescribPharmacyNum = p.PrescribPharmacyNum,
        PrescribPharmacyNPI = p.PrescribPharmacyNPI,
        PrescribPharmacyName = p.PrescribPharmacyName,
        DateFill = p.DateFill,
        DateBilled = p.DateBilled,
        DateTrans = p.DateTrans,
        RefillNum = p.RefillNum,
        NDC = p.NDC,
        DrugName = p.DrugName,
        GPI = p.GPI,
        GCN = p.GCN,
        GBrand_Flag = p.GBrand_Flag,
        GBSourceCd = None,
        TheraClassCd = p.TheraClassCd,
        Formulary_Flag = p.Formulary_Flag,
        SpecialtyDrug_Flag = p.SpecialtyDrug_Flag,
        Tier = p.Tier,
        Retail_MailOrder_Flag = p.Retail_MailOrder_Flag,
        DAWCd = p.DAWCd,
        DaysSupply = p.DaysSupply,
        PaidQty = p.PaidQty,
        ScriptCount1 = p.ScriptCount1,
        ScriptCount2 = p.ScriptCount2,
        AmtAWP = p.AmtAWP,
        AmtGross = p.AmtGross,
        AmtTierCopay = p.AmtTierCopay,
        AmtDeduct = p.AmtDeduct,
        AmtPaid = p.AmtPaid
      )
    )
  }
}
