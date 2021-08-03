package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.FacetsPharmacyMed
import cityblock.models.gold.Claims.{ClaimLineStatus, MemberIdentifier}
import cityblock.models.gold.PharmacyClaim.{
  BrandIndicator,
  Date,
  DispenseMethod,
  Drug,
  Pharmacy,
  PharmacyProvider,
  PrescribingProvider
}
import cityblock.models.gold.{Amount, Constructors, Helpers}
import cityblock.models.{Identifier, Surrogate}
import cityblock.transforms.Transform
import cityblock.transforms.Transform.Transformer
import cityblock.utilities.Conversions
import cityblock.utilities.Insurance.LineOfBusiness
import com.spotify.scio.values.SCollection

import scala.util.matching.Regex

case class PharmacyFacetsTransformer(medicare: SCollection[FacetsPharmacyMed])
    extends Transformer[Pharmacy] {
  def transform()(implicit pr: String): SCollection[Pharmacy] =
    medicare
      .groupBy(_.data.claim_number)
      .values
      .map(claims => PharmacyFacetsTransformer.mkAmountForClaim(claims.toList))
      .withName("Add gold surrogates to silver pharmacy claims")
      .map {
        case (claim, amount) =>
          val (surrogate, _) =
            Transform.addSurrogate(pr, "silver_claims_facets", table = "Pharmacy", claim)(
              _.identifier.surrogateId)
          PharmacyFacetsTransformer.mkPharmacy(claim, amount, surrogate)
      }

}

object PharmacyFacetsTransformer {
  def mkAmountForClaim(claims: List[FacetsPharmacyMed]): (FacetsPharmacyMed, Amount) = {
    val mostRecent: FacetsPharmacyMed =
      claims.maxBy(_.data.paid_date)(Transform.NoneMaxOptionLocalDateOrdering)

    val amounts: List[Amount] = claims.map(
      claim =>
        Constructors.mkAmount(
          claim.data.amnt_elig,
          claim.data.amnt_charged,
          None,
          claim.data.copay_amt,
          claim.data.claim_deductible,
          None,
          claim.data.amnt_paid
      ))

    (mostRecent, Helpers.sumAmounts(amounts))
  }

  def mkPharmacy(silverMed: FacetsPharmacyMed, amount: Amount, surrogate: Surrogate): Pharmacy =
    Pharmacy(
      identifier = mkPharmacyIdentifier(silverMed, surrogate),
      memberIdentifier = mkMemberIdentifier(silverMed),
      capitatedFlag = None,
      claimLineStatus = mkStatus(silverMed),
      cobFlag = None,
      lineOfBusiness = Some(LineOfBusiness.Medicare.toString),
      subLineOfBusiness = None,
      pharmacy = mkPharmacyProvider(silverMed),
      prescriber = mkPrescribingProvider(silverMed),
      diagnosis = None,
      drug = mkDrug(silverMed),
      amount = amount,
      date = mkDate(silverMed)
    )

  def mkPharmacyIdentifier(silverMed: FacetsPharmacyMed, surrogate: Surrogate): Identifier =
    Identifier(
      id = PharmacyKey(silverMed).uuid.toString,
      partner = partner,
      surrogate = surrogate
    )

  def mkMemberIdentifier(silverMed: FacetsPharmacyMed): MemberIdentifier =
    MemberIdentifier(
      commonId = silverMed.patient.source.commonId,
      partnerMemberId = silverMed.data.member_id,
      patientId = silverMed.patient.patientId,
      partner = partner
    )

  def mkStatus(silverMed: FacetsPharmacyMed): String =
    silverMed.data.claim_status match {
      case Some("1") => ClaimLineStatus.Paid.toString
      case Some("2") => ClaimLineStatus.Denied.toString
      case _         => ClaimLineStatus.Unknown.toString
    }

  def mkPharmacyProvider(silverMed: FacetsPharmacyMed): Option[PharmacyProvider] =
    for (num <- silverMed.data.nabp_number)
      yield
        PharmacyProvider(id = Some("unknown"), npi = None, ncpdp = Some(num), inNetworkFlag = None)

  def mkPrescribingProvider(silverMed: FacetsPharmacyMed): Option[PrescribingProvider] =
    for (num <- silverMed.data.npi_number)
      yield
        PrescribingProvider(
          id = "unknown",
          npi = Some(num),
          specialty = None,
          placeOfService = None
        )

  def mkDrug(silverMed: FacetsPharmacyMed): Drug =
    Drug(
      ndc = silverMed.data.ndc_number,
      // Quantities dispensed is not yet a stable value coming in from CCI so we take the absolute value to account for
      // instances when only the reversed lines contain quantities dispensed. Eventually we hope to sum this field as
      // the amounts are summed above to get the correct value.
      quantityDispensed = silverMed.data.quantity_dispensed
        .flatMap(Conversions.safeParse(_, BigDecimal(_)))
        .map(_.abs),
      daysSupply = silverMed.data.days_supply.flatMap(Conversions.safeParse(_, _.toInt)),
      partnerPrescriptionNumber = Some(silverMed.data.claim_number),
      fillNumber = mkFillNumber(silverMed),
      brandIndicator = BrandIndicator.Unknown.toString,
      formularyFlag = false,
      ingredient = None,
      strength = None,
      dispenseAsWritten = None,
      dispenseMethod = DispenseMethod.Unknown.toString,
      classes = List()
    )

  def mkFillNumber(silverMed: FacetsPharmacyMed): Option[Int] = {
    val oneToNine: Regex = "[1-9]".r
    val allLettersButOAndZ = "[A-NP-Y]".r

    silverMed.data.refill_code match {
      case None => None
      case Some(s) =>
        s match {
          case "O"                    => Some(0)
          case oneToNine(_*)          => Some(s.toInt)
          case allLettersButOAndZ(_*) => Some(s.charAt(0) - 'A' + 10)
          case _                      => None
        }
    }
  }

  def mkDate(silverMed: FacetsPharmacyMed): Date =
    Date(
      filled = silverMed.data.date_of_service,
      paid = silverMed.data.paid_date,
      billed = None
    )
}
