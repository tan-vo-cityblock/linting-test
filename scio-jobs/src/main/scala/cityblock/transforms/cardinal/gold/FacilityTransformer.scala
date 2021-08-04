package cityblock.transforms.cardinal.gold

import cityblock.models.CardinalSilverClaims.SilverPhysical
import cityblock.models.Surrogate
import cityblock.models.cardinal.silver.Physical.ParsedPhysical
import cityblock.models.gold.FacilityClaim._
import cityblock.models.gold.enums.ProcedureTier
import cityblock.transforms.Transform
import cityblock.transforms.Transform.CodeSet
import cityblock.utilities.Strings
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

object FacilityTransformer {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)

    val bigqueryProject: String = args.required("bigqueryProject")

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val deliveryDate: LocalDate = LocalDate.parse(args.required("deliveryDate"), dtf)

    val silverDataset = args.required("silverDataset")
    val silverTable = args.required("silverTable")

    val goldDataset = args.required("goldDataset")
    val goldTable = args.required("goldTable")

    val physicalClaims: SCollection[SilverPhysical] =
      Transform.fetchFromBigQuery[SilverPhysical](
        sc,
        bigqueryProject,
        silverDataset,
        silverTable,
        deliveryDate
      )

    val goldFacilityClaims: SCollection[Facility] = physicalClaims
      .withName("Filter for facility claims")
      .filter(isFacility)
      .withName("Add surrogates")
      .map {
        Transform.addSurrogate(
          bigqueryProject,
          silverDataset,
          silverTable,
          _
        )(_.identifier.surrogateId)
      }
      .withName("Group by claim")
      .groupBy(_._2.data.ClaimNumber)
      .withName("Construct gold facility")
      .flatMap(facRows => {
        val header = facRows._2.find(_._2.data.LineNumber == Option("1"))
        val lines = facRows._2.map {
          case (surrogate, silver) => mkLine(surrogate, silver)
        }

        mkFacilityClaim(header, lines.toList)
      })

    Transform.persist(
      goldFacilityClaims,
      bigqueryProject,
      goldDataset,
      goldTable,
      deliveryDate,
      WRITE_TRUNCATE,
      CREATE_IF_NEEDED
    )
    sc.close().waitUntilFinish()

  }

  private def mkLine(surrogate: Surrogate, silver: SilverPhysical): Line =
    Line(
      surrogate = surrogate,
      lineNumber = mkLineNumber(silver.data),
      revenueCode = Transform.addLeadingZero(silver.data.RevenueCode),
      cobFlag = mkCobFlag(silver.data.TotalThirdPartyLiabilityAmount),
      capitatedFlag = None, // TODO Cardinal to help identify
      claimLineStatus = silver.data.LineStatusCode.map(claimLineStatus),
      inNetworkFlag = mkInNetworkFlag(silver.data),
      serviceQuantity = None, // TODO Cardinal to help identify
      typesOfService = List.empty, // TODO Cardinal to help identify because values for ClaimTypeCode look off
      procedure = mkProcedure(surrogate, silver),
      amount = mkAmount(silver.data) // TODO Cardinal to help identify
    )

  private def mkFacilityClaim(silverHeader: Option[(Surrogate, SilverPhysical)],
                              silverLines: List[Line]): Option[Facility] =
    for {
      (surrogate, header) <- silverHeader
    } yield {
      Facility(
        claimId = Transform.mkIdentifier(header.data.ClaimNumber),
        memberIdentifier = mkMemberIdentifier(header.patient),
        header = mkHeader(surrogate, header),
        lines = silverLines
      )
    }

  private def mkHeader(surrogate: Surrogate, header: SilverPhysical): Header =
    Header(
      partnerClaimId = header.data.ClaimNumber,
      typeOfBill = Transform.addLeadingZero(header.data.FullBillTypeCode),
      admissionType = None, // Not receiving these three fields due to state pass-through file
      admissionSource = None,
      dischargeStatus = None,
      lineOfBusiness = mkLineOfBusiness(header.data),
      subLineOfBusiness = None,
      drg = DRG(
        version = None,
        codeset = None,
        code = header.data.DiagnosisRelatedGroupDRGCode
      ),
      provider = HeaderProvider(
        billing = mkClaimProvider(header.data.BillingNPI),
        referring = mkClaimProvider(header.data.ReferralNPI),
        servicing = mkClaimProvider(header.data.AttendingNPI),
        operating = mkClaimProvider(header.data.OperatingNPI)
      ),
      diagnoses = mkDiagnoses(surrogate, header.data),
      procedures = mkHeaderProcedures(surrogate, header.data),
      date = mkDate(header.data)
    )

  private def mkDate(data: ParsedPhysical): Date =
    Date(
      from = data.HeaderServiceStartDate,
      to = data.HeaderServiceEndDate,
      admit = data.FacilityAdmitDate,
      discharge = data.FacilityDischargeDate,
      paid = data.PaidDate
    )

  private def mkHeaderProcedures(surrogate: Surrogate,
                                 silver: ParsedPhysical): List[HeaderProcedure] = {
    val maybeProcedures = silver.ProcedureCode.map { code =>
      HeaderProcedure(
        surrogate = surrogate,
        tier = ProcedureTier.Principal.toString,
        codeset = mkCodeset(code),
        code = code
      )
    }

    List(maybeProcedures).flatten
  }

  private def mkCodeset(code: String): String =
    if (Strings.startsWithCapitalLetter(code)) {
      CodeSet.HCPCS.toString
    } else {
      CodeSet.CPT.toString
    }
}
