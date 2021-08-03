package cityblock.transforms.healthyblue.gold

import java.util.UUID

import cityblock.models.HealthyBlueSilverClaims.{SilverPharmacyHeader, SilverPharmacyLine}
import cityblock.models.gold.PharmacyClaim._
import cityblock.models.gold.{Amount, Constructors}
import cityblock.models.healthyblue.silver.PharmacyHeader.ParsedPharmacyHeader
import cityblock.models.healthyblue.silver.PharmacyLines.ParsedPharmacyLine
import cityblock.models.{Identifier, Surrogate}
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
    val silverHeaderTable = args.required("silverHeaderTable")
    val silverLinesTable = args.required("silverLinesTable")

    val goldDataset = args.required("goldDataset")
    val goldTable = args.required("goldTable")

    val silverHeader: SCollection[SilverPharmacyHeader] =
      Transform.fetchFromBigQuery[SilverPharmacyHeader](
        sc,
        bigqueryProject,
        silverDataset,
        silverHeaderTable,
        deliveryDate
      )

    val silverLines: SCollection[SilverPharmacyLine] =
      Transform.fetchFromBigQuery[SilverPharmacyLine](
        sc,
        bigqueryProject,
        silverDataset,
        silverLinesTable,
        deliveryDate
      )

    val joinedHeaderLines: SCollection[(SilverPharmacyHeader, SilverPharmacyLine)] =
      silverHeader
        .keyBy(_.data.Transaction_Control_Number)
        .join(silverLines.keyBy(_.data.Transaction_Control_Number))
        .values

    val goldPharmacy: SCollection[Pharmacy] =
      joinedHeaderLines
        .withName("Add surrogates")
        .map {
          Transform.addSurrogate(
            bigqueryProject,
            silverDataset,
            silverHeaderTable,
            _
          )(_._1.identifier.surrogateId)
        }
        .withName("Create gold pharmacy claims")
        .map {
          case (surrogate, (silverHeader, silverLines)) =>
            mkPharmacy(surrogate, silverHeader, silverLines)
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

  private def mkPharmacy(surrogate: Surrogate,
                         silverHeader: SilverPharmacyHeader,
                         silverLine: SilverPharmacyLine): Pharmacy =
    Pharmacy(
      identifier = mkIdentifier(surrogate, silverHeader, silverLine),
      memberIdentifier = Transform.mkMemberIdentifierForPatient(silverHeader.patient, partner),
      capitatedFlag = None,
      claimLineStatus = mkClaimLineStatus(silverLine.data.Line_Status_Code),
      cobFlag = None, // TODO ask HB
      lineOfBusiness = Some(LineOfBusiness.Medicaid.toString),
      subLineOfBusiness = None, // TODO figure this out?
      pharmacy = mkPharmacyProvider(silverHeader),
      prescriber = mkPrescriberProvider(silverLine),
      diagnosis = None,
      drug = mkDrug(silverLine.data),
      amount = mkAmount(silverLine.data),
      date = mkDate(silverHeader.data)
    )

  private def mkIdentifier(surrogate: Surrogate,
                           silverHeader: SilverPharmacyHeader,
                           silverLine: SilverPharmacyLine): Identifier =
    Identifier(
      id = UUID.nameUUIDFromBytes(mkPharmacyKey(silverHeader, silverLine).getBytes()).toString,
      partner = partner,
      surrogate = surrogate
    )

  private def mkPharmacyKey(silverHeader: SilverPharmacyHeader,
                            silverLine: SilverPharmacyLine): String =
    List(
      Some(partner),
      Some(silverLine.data.Transaction_Control_Number),
      Some(silverLine.data.Line_Number),
      Some(silverHeader.patient.externalId),
      silverHeader.data.Date_of_Service
    ).flatten.toString

  private def mkPharmacyProvider(silverHeader: SilverPharmacyHeader): Option[PharmacyProvider] =
    for (npi <- silverHeader.data.Service_Provider_ID)
      yield
        PharmacyProvider(id = None, npi = Some(npi), ncpdp = None, inNetworkFlag = None) // TODO ask HB

  private def mkPrescriberProvider(silverLine: SilverPharmacyLine): Option[PrescribingProvider] =
    for (npi <- silverLine.data.Prescribing_Provider_NPI)
      yield
        PrescribingProvider(
          id = "unknown",
          npi = Some(npi),
          specialty = None,
          placeOfService = silverLine.data.Level_Of_Service
        )

  private def mkDrug(parsedLine: ParsedPharmacyLine): Drug =
    Drug(
      ndc = parsedLine.Compound_Product_Or_NDC_Code,
      quantityDispensed =
        parsedLine.Quantity_Dispensed.flatMap(Conversions.safeParse(_, BigDecimal(_))),
      daysSupply = parsedLine.Days_Supply.flatMap(Conversions.safeParse(_, _.toInt)),
      partnerPrescriptionNumber = parsedLine.Prescription_Number,
      fillNumber = parsedLine.Fill_Number.flatMap(Conversions.safeParse(_, _.toInt)),
      brandIndicator = BrandIndicator.Unknown.toString,
      ingredient = parsedLine.Compound_Ingredient_Basis_Of_Cost_Determination,
      strength = None,
      dispenseAsWritten = parsedLine.Dispense_As_Written_or_Product_Selection_Code,
      dispenseMethod = DispenseMethod.Unknown.toString,
      classes = List(),
      formularyFlag = false
    )

  private def mkAmount(line: ParsedPharmacyLine): Amount =
    Constructors.mkAmount(
      allowed = line.Claim_Allowed_Amount,
      billed = line.Total_Claim_Charge_Amount,
      COB = None,
      copay = None,
      deductible = None,
      coinsurance = None,
      planPaid = line.Payers_Claim_Payment_Amount
    )

  private def mkDate(header: ParsedPharmacyHeader): Date =
    Date(
      filled = header.Date_of_Service,
      paid = header.Date_of_Payment,
      billed = header.Date_of_Receipt
    )
}
