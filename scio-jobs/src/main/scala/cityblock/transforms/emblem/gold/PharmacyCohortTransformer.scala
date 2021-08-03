package cityblock.transforms.emblem.gold

import cityblock.models.EmblemSilverClaims.SilverPharmacyClaimCohort
import cityblock.models.gold.Claims._
import cityblock.models.gold.PharmacyClaim._
import cityblock.models.gold.{Amount, Constructors}
import cityblock.models.{Identifier, Surrogate}
import cityblock.transforms.Transform
import cityblock.transforms.Transform.Transformer
import cityblock.utilities.emblem.Insurance
import cityblock.utilities.reference.tables
import cityblock.utilities.{Conversions, Loggable}
import com.spotify.scio.values.{SCollection, SideInput}
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

case class PharmacyCohortTransformer(
  claims: SCollection[SilverPharmacyClaimCohort],
  mappedLOB: SideInput[Map[String, tables.LineOfBusiness]],
  deliveryDate: LocalDate
) extends Transformer[Pharmacy] {
  def transform()(implicit pr: String): SCollection[Pharmacy] =
    PharmacyCohortTransformer.mkPharmacy(claims, pr, mappedLOB, deliveryDate)
}

object PharmacyCohortTransformer extends Loggable {
  def mkPharmacy(
    pharmacySilvers: SCollection[SilverPharmacyClaimCohort],
    project: String,
    mappedLOB: SideInput[Map[String, tables.LineOfBusiness]],
    deliveryDate: LocalDate
  ): SCollection[Pharmacy] = {
    val transformed: SCollection[Pharmacy] = pharmacySilvers
      .withSideInputs(mappedLOB)
      .map { (silver, ctx) =>
        val dictLob = ctx(mappedLOB)

        val (surrogate, _) =
          Transform.addSurrogate(project, "silver_claims", "pharmacy", silver)(
            _.identifier.surrogateId)

        Pharmacy(
          identifier = mkPharmacyIdentifier(surrogate),
          memberIdentifier = mkMemberIdentifier(silver: SilverPharmacyClaimCohort),
          capitatedFlag = None,
          claimLineStatus = claimLineStatus(silver),
          cobFlag = cobFlag(silver),
          lineOfBusiness = Insurance.PharmacyCohortMapping
            .getLineOfBusiness(silver.claim, dictLob),
          subLineOfBusiness = Insurance.PharmacyCohortMapping
            .getSubLineOfBusiness(silver.claim, dictLob),
          pharmacy = Some(mkPharmacyProvider(silver)),
          prescriber = mkPrescribingProvider(silver),
          diagnosis = None,
          drug = mkDrug(silver),
          amount = mkAmount(silver),
          date = mkDate(silver, deliveryDate)
        )
      }
      .toSCollection

    fixFillNumber(transformed)
  }

  // NOTE: To assign fill numbers to any "Z"s from the silver fill number field which have now all been matched to
  // fill number 34, we use the claim paid date to order and assign numbers in ascending order across different claims
  // for the same prescription ID to fix and assign the correct fill number.
  private[gold] def fixFillNumber(pharmacies: SCollection[Pharmacy]): SCollection[Pharmacy] = {
    val ZFillNumberIndex: Int = 'Z' - 'A' + 9

    val (claimsToFix, claimsAlreadyGood) =
      pharmacies.partition(_.drug.fillNumber.contains(ZFillNumberIndex))

    val modifiedClaims: SCollection[Pharmacy] = claimsToFix
      .groupBy(_.drug.partnerPrescriptionNumber)
      .flatMap {
        case (_, pharmacies) =>
          val pharmaciesOrdered: Seq[Pharmacy] = pharmacies.toSeq
            .sortBy(_.date.billed)(Transform.NoneMaxOptionLocalDateOrdering)

          pharmaciesOrdered.zipWithIndex.map {
            case (pharmacy, index) =>
              pharmacy.copy(drug = pharmacy.drug.copy(fillNumber = Some(ZFillNumberIndex + index)))
          }
      }

    modifiedClaims ++ claimsAlreadyGood
  }

  private[gold] def mkPharmacyIdentifier(surrogate: Surrogate): Identifier =
    Identifier(
      id = Transform.generateUUID(),
      partner = partner,
      surrogate = surrogate
    )

  private[gold] def mkMemberIdentifier(silver: SilverPharmacyClaimCohort): MemberIdentifier =
    MemberIdentifier(
      commonId = silver.patient.source.commonId,
      partnerMemberId = silver.patient.externalId,
      patientId = silver.patient.patientId,
      partner = partner
    )

  private[gold] def mkPharmacyProvider(silver: SilverPharmacyClaimCohort): PharmacyProvider =
    // In pharmacy claims only, Emblem stores an NPI in BILL_PROV (instead of their own provider id)
    PharmacyProvider(id = None,
                     npi = silver.claim.BILL_PROV,
                     ncpdp = None,
                     inNetworkFlag = silver.claim.CLAIM_IN_NETWORK)

  private[gold] def mkPrescribingProvider(
    silver: SilverPharmacyClaimCohort): Option[PrescribingProvider] =
    // In pharmacy claims only, Emblem stores an NPI in ATT_PROV (instead of their own provider id)
    for (npi <- silver.claim.ATT_PROV)
      yield
        PrescribingProvider(
          id = "unknown",
          npi = Some(npi),
          specialty = silver.claim.ATT_PROV_SPEC,
          placeOfService = silver.claim.POS
        )

  private[gold] def mkDrug(silver: SilverPharmacyClaimCohort): Drug =
    Drug(
      ndc = silver.claim.NDC,
      quantityDispensed = silver.claim.RX_QTY_DISPENSED
        .flatMap(Conversions.safeParse(_, BigDecimal(_))),
      daysSupply = silver.claim.RX_DAYS_SUPPLY.flatMap(Conversions.safeParse(_, _.toInt)),
      partnerPrescriptionNumber = silver.claim.RX_NO,
      fillNumber = fillNumber(silver),
      brandIndicator = brandIndicator(silver).toString,
      ingredient = silver.claim.GRP_DRUG_NAME,
      strength = None,
      dispenseAsWritten = dispenseAsWritten(silver),
      dispenseMethod = dispenseMethod(silver).toString,
      classes = classes(silver),
      formularyFlag = silver.claim.RX_FORM.contains("Y")
    )

  private[gold] def mkAmount(silver: SilverPharmacyClaimCohort): Amount =
    Constructors.mkAmount(
      allowed = silver.claim.AMT_ALLOWED,
      billed = silver.claim.AMT_BILLED,
      COB = silver.claim.AMT_COB,
      copay = silver.claim.AMT_COPAY,
      deductible = silver.claim.AMT_DEDUCT,
      coinsurance = silver.claim.AMT_COINS,
      planPaid = silver.claim.AMT_PAID
    )

  /**
   * Starting in July 2019, FROM_DATE no longer represented the filled date.
   * Business decision was made to infer the fill date as the middle of the SERVICEYEARMONTH.
   */
  private[gold] def mkDate(silver: SilverPharmacyClaimCohort, deliveryDate: LocalDate): Date = {
    val fillDate =
      if (deliveryDate.isAfter(new LocalDate("2019-06-12"))) {
        silver.claim.SERVICEYEARMONTH.flatMap { yearMonth =>
          val middleMonth = yearMonth + "15"
          val formatter = DateTimeFormat.forPattern("yyyyMMdd")

          val parsedDate = Conversions.safeParse(middleMonth, LocalDate.parse(_, formatter))

          if (parsedDate.isEmpty) {
            logger.warn(
              "Couldn't parse SERVICEYEARMONTH for row with SurrogateId " + silver.identifier.surrogateId)
          }

          parsedDate
        }
      } else { silver.claim.FROM_DATE }

    Date(filled = fillDate, paid = silver.claim.PAID_DATE, billed = silver.claim.CLAIM_REC_DATE)

  }

  private[gold] def claimLineStatus(silver: SilverPharmacyClaimCohort): String =
    silver.claim.SV_STAT match {
      case Some("1") => ClaimLineStatus.Paid.toString
      case Some("2") => ClaimLineStatus.Unknown.toString
      case s =>
        logger.error(s"Received unexpected value in Emblem pharmacy claim: SV_STAT = $s")
        ClaimLineStatus.Unknown.toString
    }

  private[gold] def cobFlag(silver: SilverPharmacyClaimCohort): Option[Boolean] =
    silver.claim.AMT_COB
      .flatMap(Conversions.safeParse(_, BigDecimal(_))) match {
      case Some(cob) => Some(cob != 0)
      case _         => None
    }

  // NOTE: Pharmacy fill numbers are assigned as following: O, 1,...,9,A,...,N,P,...Y,Z,...,Z
  // "O" maps to 0 so it is excluded from the alphabetic section. See the fixFillNumber function for further detail.
  private[gold] def fillNumber(silver: SilverPharmacyClaimCohort): Option[Int] = {
    val oneToNine = "[1-9]".r
    val allLettersUpToO = "[A-N]".r
    val allLettersAfterO = "[P-Z]".r

    silver.claim.RX_REFILLS match {
      case None => None
      case Some(s) =>
        s match {
          case "O"                  => Some(0)
          case oneToNine(_*)        => Some(s.toInt)
          case allLettersUpToO(_*)  => Some(s.charAt(0) - 'A' + 10)
          case allLettersAfterO(_*) => Some(s.charAt(0) - 'A' + 9)
          case _                    => None
        }
    }
  }

  private[gold] def brandIndicator(silver: SilverPharmacyClaimCohort): BrandIndicator.Value =
    silver.claim.BRAND_CODE match {
      case Some("B") => BrandIndicator.Brand
      case Some("G") => BrandIndicator.Generic
      case _         => BrandIndicator.Unknown
    }

  private[gold] def dispenseAsWritten(silver: SilverPharmacyClaimCohort): Option[String] = {
    val digit = "[0-9]".r
    silver.claim.RX_DAW match {
      case None => None
      case Some(daw) =>
        daw match {
          case digit() => Some(daw)
          case "O"     => Some("0")
          case _       => None
        }
    }
  }

  private[gold] def dispenseMethod(silver: SilverPharmacyClaimCohort): DispenseMethod.Value =
    silver.claim.RX_FILL_SRC match {
      case Some("R") => DispenseMethod.Retail
      case Some("M") => DispenseMethod.Mail
      case _         => DispenseMethod.Unknown
    }

  private[gold] def classes(silver: SilverPharmacyClaimCohort): List[DrugClass] =
    silver.claim.GC3_CODE.map { code =>
      DrugClass(code = code, codeset = DrugClassCodeset.GC3.toString)
    }.toList

}
