package cityblock.transforms.carefirst.gold

import cityblock.models.CarefirstSilverClaims.SilverPharmacy
import cityblock.models.carefirst.silver.Pharmacy.ParsedPharmacy
import cityblock.models.gold.{Amount, Constructors}
import cityblock.models.gold.Claims.{ClaimLineStatus, MemberIdentifier}
import cityblock.models.{Identifier, Surrogate}
import cityblock.models.gold.PharmacyClaim.{
  BrandIndicator,
  Date,
  DispenseMethod,
  Drug,
  DrugClass,
  DrugClassCodeset,
  Pharmacy,
  PharmacyProvider,
  PrescribingProvider
}
import cityblock.transforms.Transform
import cityblock.transforms.Transform.Transformer
import cityblock.utilities.Insurance.LineOfBusiness
import cityblock.utilities.{Conversions, Loggable}
import com.spotify.scio.values.SCollection

case class PharmacyTransformer(pharmacy: SCollection[SilverPharmacy])
    extends Transformer[Pharmacy] {
  def transform()(implicit pr: String): SCollection[Pharmacy] =
    pharmacy
      .withName("Add surrogates")
      .map {
        Transform.addSurrogate(pr, "silver_claims", "pharmacy", _)(_.identifier.surrogateId)
      }
      .map {
        case (surrogate, silver) => PharmacyTransformer.mkPharmacy(surrogate, silver)
      }
}

object PharmacyTransformer extends Loggable {
  def mkPharmacy(surrogate: Surrogate, silver: SilverPharmacy): Pharmacy =
    Pharmacy(
      identifier = Identifier(
        id = ClaimKey(silver).uuid.toString,
        partner = partner,
        surrogate = surrogate
      ),
      memberIdentifier = MemberIdentifier(
        commonId = silver.patient.source.commonId,
        partnerMemberId = silver.patient.externalId,
        patientId = silver.patient.patientId,
        partner = partner
      ),
      capitatedFlag = None,
      claimLineStatus = silver.data.CLAIM_LINE_STATUS
        .map(claimLineStatus)
        .getOrElse(ClaimLineStatus.Unknown.toString),
      cobFlag = mkCobFlag(silver.data),
      lineOfBusiness = Some(LineOfBusiness.Medicaid.toString),
      subLineOfBusiness = Some("Supplemental Security Income"),
      pharmacy = Some(mkPharmacyProvider(silver.data)),
      prescriber = mkPrescriberProvider(silver.data),
      diagnosis = None,
      drug = mkDrug(silver.data),
      amount = mkAmount(silver.data),
      date = mkDate(silver.data)
    )

  private[gold] def mkCobFlag(silver: ParsedPharmacy): Option[Boolean] =
    silver.AMT_COB.flatMap(Conversions.safeParse(_, BigDecimal(_))) match {
      case Some(cob) => Some(cob != 0)
      case _         => None
    }

  private[gold] def mkPharmacyProvider(silver: ParsedPharmacy): PharmacyProvider =
    PharmacyProvider(id = None,
                     npi = silver.BILL_PROV,
                     ncpdp = None,
                     inNetworkFlag = silver.CLAIM_IN_NETWORK)

  private[gold] def mkPrescriberProvider(silver: ParsedPharmacy): Option[PrescribingProvider] =
    for (npi <- silver.ATT_PROV)
      yield
        PrescribingProvider(
          id = "unknown",
          npi = Some(npi),
          specialty = silver.ATT_PROV_SPEC,
          placeOfService = silver.POS
        )

  private[gold] def mkDrug(silver: ParsedPharmacy): Drug =
    Drug(
      ndc = silver.NDC,
      quantityDispensed = silver.RX_QTY_DISPENSED.flatMap(Conversions.safeParse(_, BigDecimal(_))),
      daysSupply = silver.RX_DAYS_SUPPLY.flatMap(Conversions.safeParse(_, _.toInt)),
      partnerPrescriptionNumber = silver.RX_NO,
      fillNumber = silver.RX_REFILLS.flatMap(Conversions.safeParse(_, _.toInt)),
      brandIndicator = mkBrandIndicator(silver).toString,
      ingredient = silver.GRP_DRUG_NAME,
      strength = None,
      dispenseAsWritten = silver.RX_DAW,
      dispenseMethod = mkDispenseMethod(silver).toString,
      classes = mkClasses(silver),
      formularyFlag = silver.RX_FORM.contains("Y")
    )

  private[gold] def mkBrandIndicator(silver: ParsedPharmacy): BrandIndicator.Value =
    silver.BRAND_CODE match {
      case Some("B") => BrandIndicator.Brand
      case Some("G") => BrandIndicator.Generic
      case _         => BrandIndicator.Unknown
    }

  private[gold] def mkDispenseMethod(silver: ParsedPharmacy): DispenseMethod.Value =
    silver.RX_FILL_SRC match {
      case Some("R") => DispenseMethod.Retail
      case Some("M") => DispenseMethod.Mail
      case _         => DispenseMethod.Unknown
    }

  private[gold] def mkClasses(silver: ParsedPharmacy): List[DrugClass] =
    silver.GC3_CODE.map { code =>
      DrugClass(code = code, codeset = DrugClassCodeset.GC3.toString)
    }.toList

  private[gold] def mkAmount(silver: ParsedPharmacy): Amount =
    Constructors.mkAmount(
      allowed = silver.AMT_ALLOWED,
      billed = silver.AMT_BILLED,
      COB = silver.AMT_COB,
      copay = silver.AMT_COPAY,
      deductible = silver.AMT_DEDUCT,
      coinsurance = silver.AMT_COINS,
      planPaid = silver.AMT_PAID
    )

  private[gold] def mkDate(silver: ParsedPharmacy): Date =
    Date(
      filled = silver.FROM_DATE,
      paid = silver.PAID_DATE,
      billed = silver.CLAIM_REC_DATE
    )
}
