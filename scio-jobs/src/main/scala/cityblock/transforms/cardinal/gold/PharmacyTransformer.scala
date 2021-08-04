package cityblock.transforms.cardinal.gold

import java.util.UUID

import cityblock.models.CardinalSilverClaims.SilverPharmacy
import cityblock.models.cardinal.silver.Pharmacy.ParsedPharmacy
import cityblock.models.gold.{Amount, Constructors}
import cityblock.models.gold.Claims.ClaimLineStatus
import cityblock.models.{Identifier, Surrogate}
import cityblock.models.gold.PharmacyClaim.{
  BrandIndicator,
  Date,
  DispenseMethod,
  Drug,
  Pharmacy,
  PharmacyProvider,
  PrescribingProvider
}
import cityblock.transforms.Transform
import cityblock.utilities.Conversions
import cityblock.utilities.Insurance.LineOfBusiness
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

object PharmacyTransformer {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)

    val bigqueryProject: String = args.required("bigqueryProject")

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val deliveryDate: LocalDate = LocalDate.parse(args.required("deliveryDate"), dtf)

    val silverDataset = args.required("silverDataset")
    val silverTable = args.required("silverTable")

    val goldDataset = args.required("goldDataset")
    val goldTable = args.required("goldTable")

    val silverPharmacy: SCollection[SilverPharmacy] =
      Transform.fetchFromBigQuery[SilverPharmacy](
        sc,
        bigqueryProject,
        silverDataset,
        silverTable,
        deliveryDate
      )

    val goldPharmacy = silverPharmacy
      .withName("Add surrogates")
      .map {
        Transform.addSurrogate(
          bigqueryProject,
          silverDataset,
          silverTable,
          _
        )(_.identifier.surrogateId)
      }
      .withName("Construct pharmacy gold results")
      .map {
        case (surrogate, silver) =>
          PharmacyTransformer.mkPharmacy(surrogate, silver)
      }

    Transform.persist(
      goldPharmacy,
      bigqueryProject,
      goldDataset,
      goldTable,
      deliveryDate,
      WRITE_TRUNCATE,
      CREATE_IF_NEEDED
    )

    sc.close().waitUntilFinish()
  }

  private def mkPharmacy(surrogate: Surrogate, silver: SilverPharmacy): Pharmacy =
    Pharmacy(
      identifier = mkIdentifier(surrogate, silver),
      memberIdentifier = mkMemberIdentifier(silver.patient),
      capitatedFlag = None,
      claimLineStatus = mkClaimStatus(silver.data),
      cobFlag = mkCobFlag(silver.data.TotalThirdPartyLiabilityAmount), // TODO double check that TotalThirdPartyLiabilityAmount works
      lineOfBusiness = Some(LineOfBusiness.Medicaid.toString),
      subLineOfBusiness = None,
      pharmacy = Some(mkPharmacyProvider(silver.data)),
      prescriber = Some(mkPrescribingProvider(silver.data)),
      diagnosis = None, // TODO wait for cardinal response about DiagnosisCode1 and Diagnosis1
      drug = mkDrug(silver.data),
      amount = mkAmount(silver.data),
      date = mkDate(silver.data)
    )

  private def mkIdentifier(surrogate: Surrogate, silver: SilverPharmacy): Identifier =
    Identifier(
      id = UUID.nameUUIDFromBytes(mkPharmacyKey(silver).getBytes()).toString,
      partner = partner,
      surrogate = surrogate
    )

  private def mkPharmacyKey(silver: SilverPharmacy): String =
    List(
      Some(partner),
      Some(silver.data.ClaimNumber),
      Some(silver.patient.externalId),
      silver.data.FillDate
    ).flatten.toString

  private def mkClaimStatus(silver: ParsedPharmacy): String =
    silver.HeaderStatusCode match {
      case Some("P") =>
        ClaimLineStatus.Paid.toString
      case _ => ClaimLineStatus.Unknown.toString
    }

  private def mkPharmacyProvider(silver: ParsedPharmacy): PharmacyProvider =
    PharmacyProvider(
      id = silver.BillingNPI.map(mkProviderId),
      npi = silver.BillingNPI,
      ncpdp = None,
      inNetworkFlag = None
    )

  private def mkPrescribingProvider(silver: ParsedPharmacy): PrescribingProvider =
    PrescribingProvider(
      id = silver.PrescriberNPI.map(mkProviderId).getOrElse("unknown"),
      npi = silver.PrescriberNPI,
      specialty = None,
      placeOfService = None
    )

  private def mkDrug(silver: ParsedPharmacy): Drug =
    Drug(
      ndc = silver.DrugCode,
      quantityDispensed = silver.SupplyQuantity.flatMap(Conversions.safeParse(_, BigDecimal(_))),
      daysSupply = silver.DaysSupplyCount.flatMap(Conversions.safeParse(_, _.toInt)),
      partnerPrescriptionNumber = silver.PrescriptionNumber,
      fillNumber = silver.RefillCount.flatMap(Conversions.safeParse(_, _.toInt)),
      brandIndicator = BrandIndicator.Unknown.toString,
      ingredient = silver.DrugTradeName,
      strength = silver.DrugStrength,
      dispenseAsWritten = None,
      dispenseMethod = DispenseMethod.Unknown.toString,
      classes = List(),
      formularyFlag = false
    )

  private def mkAmount(silver: ParsedPharmacy): Amount =
    Constructors.mkAmount(
      allowed = silver.LineNetPayableAmount,
      billed = None,
      COB = silver.TotalThirdPartyLiabilityAmount, // TODO make sure TotalThirdPartyLiabilityAmount works
      copay = None,
      deductible = None,
      coinsurance = None,
      planPaid = silver.LineNetPayableAmount
    )

  private def mkDate(silver: ParsedPharmacy): Date =
    Date(
      filled = silver.FillDate,
      paid = silver.PaidDate,
      billed = None
    )
}
