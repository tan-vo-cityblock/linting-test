package cityblock.transforms.tufts.gold

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.models.{Identifier, Surrogate}
import cityblock.models.TuftsSilverClaims.SilverPharmacy
import cityblock.models.gold.{Amount, Helpers}
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
import cityblock.models.tufts.silver.Pharmacy.ParsedPharmacy
import cityblock.transforms.Transform
import cityblock.transforms.Transform.Transformer
import cityblock.transforms.tufts.{ClaimKey, ProviderKey}
import cityblock.utilities.Conversions
import cityblock.utilities.Insurance.{LineOfBusiness, SubLineOfBusiness}
import com.spotify.scio.values.SCollection

case class PharmacyTransformer(
  pharmacy: SCollection[SilverPharmacy]
) extends Transformer[Pharmacy] {
  override def transform()(implicit pr: String): SCollection[Pharmacy] =
    PharmacyTransformer.pipeline(pharmacy, pr)
}

object PharmacyTransformer {
  def pipeline(
    silverPharmacies: SCollection[SilverPharmacy],
    project: String
  ): SCollection[Pharmacy] = {
    val keyedPharmacies: SCollection[(ClaimKey, SilverPharmacy)] =
      silverPharmacies.map(silverPharmacy => {
        val memberId = silverPharmacy.patient.externalId
        val prescriptionNumber = silverPharmacy.data.Script_number
        (ClaimKey(memberId, prescriptionNumber), silverPharmacy)
      })

    val groupedLines: SCollection[(ClaimKey, Iterable[SilverPharmacy])] =
      keyedPharmacies.groupByKey

    mkPharmacy(groupedLines, project)
  }

  private def mkClaimLineStatus(line: ParsedPharmacy): String = ClaimLineStatus.Paid.toString

  private def mkCobFlag(line: ParsedPharmacy): Option[Boolean] =
    line.Coordination_of_Benefits_or_TPL_Liability_Amount
      .flatMap(Conversions.safeParse(_, _.toInt))
      .map(_ > 0)

  private def mkDaysSupply(line: ParsedPharmacy): Option[Int] =
    line.Days_Supply.flatMap(Conversions.safeParse(_, _.toInt))

  private def mkFillNumber(line: ParsedPharmacy): Option[Int] =
    line.New_Prescription_or_Refill.flatMap(Conversions.safeParse(_, _.toInt))

  private def mkBrandIndicator(pharmacy: ParsedPharmacy): String = {
    val brandIndicator = pharmacy.Generic_Drug_Indicator match {
      case Some("1") => BrandIndicator.Generic
      case Some("2") => BrandIndicator.Brand
      case _         => BrandIndicator.Unknown
    }
    brandIndicator.toString
  }

  private def mkFormularyFlag(line: ParsedPharmacy): Boolean = line.Formulary_Code.contains("1")

  private def mkDispenseMethod(line: ParsedPharmacy): String =
    line.Mail_Order_pharmacy match {
      case Some("1") => DispenseMethod.Mail.toString
      case Some("3") => DispenseMethod.Unknown.toString
      case Some("5") => DispenseMethod.NotApplicable.toString
      case _         => DispenseMethod.Unknown.toString
    }

  private def mkDrug(pharmacy: ParsedPharmacy, quantityDispensed: BigDecimal): Drug =
    Drug(
      ndc = pharmacy.Drug_Code,
      quantityDispensed = Some(quantityDispensed),
      daysSupply = mkDaysSupply(pharmacy),
      partnerPrescriptionNumber = Some(pharmacy.Script_number),
      fillNumber = mkFillNumber(pharmacy),
      brandIndicator = mkBrandIndicator(pharmacy),
      ingredient = pharmacy.Drug_Name,
      strength = None,
      dispenseAsWritten = pharmacy.Dispense_as_Written_Code,
      dispenseMethod = mkDispenseMethod(pharmacy),
      classes = List.empty,
      formularyFlag = mkFormularyFlag(pharmacy)
    )

  private def mkDate(line: ParsedPharmacy): Date =
    Date(
      filled = line.Date_Prescription_Filled,
      paid = line.Paid_Date,
      billed = None
    )

  private def mkIdentifier(key: ClaimKey, surrogate: Surrogate): Identifier =
    Identifier(
      id = key.uuid.toString,
      partner = partner,
      surrogate = surrogate
    )

  private def mkMemberIdentifier(member: Patient, pharmacy: ParsedPharmacy): MemberIdentifier =
    MemberIdentifier(
      commonId = None,
      partnerMemberId = pharmacy.Carrier_Specific_Unique_Member_ID,
      patientId = member.patientId,
      partner = partner
    )

  private def mkPharmacyProvider(line: ParsedPharmacy): Option[PharmacyProvider] =
    Some(
      PharmacyProvider(
        id = None,
        npi = line.Prescribing_Physician_NPI,
        ncpdp = None,
        inNetworkFlag = None
      )
    )

  private def mkPrescribingProvider(line: ParsedPharmacy): Option[PrescribingProvider] =
    line.Prescribing_Provider_ID.map(id => {
      PrescribingProvider(
        id = ProviderKey(id).uuid.toString,
        npi = line.Prescribing_Physician_NPI,
        specialty = None,
        placeOfService = None
      )
    })

  private def convertAmount(amount: Option[String]): Option[BigDecimal] =
    amount
      .flatMap(Conversions.safeParse(_, BigDecimal(_)))
      .map(_ / BigDecimal(100.0))

  private def mkQuantityDispensed(line: ParsedPharmacy): Option[BigDecimal] =
    line.Quantity_Dispensed.flatMap(Conversions.safeParse(_, BigDecimal(_)))

  private def mkAmountForClaim(
    pharmacy: Iterable[SilverPharmacy]
  ): Iterable[(SilverPharmacy, Amount, BigDecimal)] =
    // NOTE: Tufts sends us multiple claim lines per pharmacy claim, including "reversed" lines. We need to sum all
    // amounts and quantities dispensed over every line to get the accurate claim amount information. We then pick the
    // most recent claim line to populate the remaining fields.
    pharmacy
      .groupBy(_.data.New_Prescription_or_Refill)
      .values
      .map(linesByRefill => {
        val lines = linesByRefill.toList
        val mostRecent = lines.maxBy(_.data.Paid_Date)(Transform.NoneMaxOptionLocalDateOrdering)

        val amounts: List[Amount] = lines.map(line => {
          val data = line.data
          val paid: Option[BigDecimal] = convertAmount(data.Paid_Amount)
          val copay: Option[BigDecimal] = convertAmount(data.Copay_Amount)
          val coinsurance: Option[BigDecimal] = convertAmount(data.Coinsurance_Amount)
          Amount(
            Helpers.sumAmount(List(paid, copay, coinsurance)),
            convertAmount(data.Charge_Amount),
            convertAmount(data.Coordination_of_Benefits_or_TPL_Liability_Amount),
            copay,
            convertAmount(data.Deductible_Amount),
            coinsurance,
            paid
          )
        })

        val quantitiesDispensed: BigDecimal =
          linesByRefill.flatMap(pharmacy => mkQuantityDispensed(pharmacy.data)).sum

        (mostRecent, Helpers.sumAmounts(amounts), quantitiesDispensed)
      })

  def mkPharmacy(
    silverPharmacy: SCollection[(ClaimKey, Iterable[SilverPharmacy])],
    project: String
  ): SCollection[Pharmacy] =
    silverPharmacy.flatMap {
      case (claimKey, claimLines) =>
        val pharmacyToTransform: Iterable[(SilverPharmacy, Amount, BigDecimal)] =
          mkAmountForClaim(claimLines)

        pharmacyToTransform.map {
          case (claimLine, amounts, quantityDispensed) =>
            val silverData: ParsedPharmacy = claimLine.data
            val surrogate: Surrogate =
              Transform
                .addSurrogate(project, "silver_claims", "PharmacyClaim", claimLine)(
                  _.identifier.surrogateId
                )
                ._1

            Pharmacy(
              identifier = mkIdentifier(claimKey, surrogate),
              memberIdentifier = mkMemberIdentifier(claimLine.patient, silverData),
              capitatedFlag = None,
              claimLineStatus = mkClaimLineStatus(silverData),
              cobFlag = mkCobFlag(silverData),
              lineOfBusiness = Some(LineOfBusiness.Medicare.toString),
              subLineOfBusiness = Some(SubLineOfBusiness.Dual.toString),
              pharmacy = mkPharmacyProvider(silverData),
              prescriber = mkPrescribingProvider(silverData),
              diagnosis = None,
              drug = mkDrug(silverData, quantityDispensed),
              amount = amounts,
              date = mkDate(silverData)
            )
        }
    }
}
