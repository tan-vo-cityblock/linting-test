package cityblock.transforms.cardinal.gold

import cityblock.models.CardinalSilverClaims.SilverPhysical
import cityblock.models.Surrogate
import cityblock.models.cardinal.silver.Physical.ParsedPhysical
import cityblock.models.gold.ProfessionalClaim._
import cityblock.transforms.Transform
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

object ProfessionalTransformer {
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

    val goldProfessional: SCollection[Professional] = physicalClaims
      .withName("Filter out facility claims")
      .filter(isProfessional)
      .withName("Add surrogates")
      .map {
        Transform.addSurrogate(
          bigqueryProject,
          silverDataset,
          silverTable,
          _
        )(_.identifier.surrogateId)
      }
      .withName("Group by ClaimNumber")
      .groupBy(_._2.data.ClaimNumber)
      .withName("Create professional gold")
      .flatMap(profRows => {
        val headerSilver = profRows._2.find(_._2.data.LineNumber == Option("1"))
        val lines = profRows._2.map {
          case (surrogate, silver) => mkLine(surrogate, silver)
        }

        mkProfessional(headerSilver, lines.toList)
      })

    Transform.persist(
      goldProfessional,
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
      cobFlag = mkCobFlag(silver.data.TotalThirdPartyLiabilityAmount),
      capitatedFlag = None, // TODO Cardinal to help identify
      claimLineStatus = silver.data.LineStatusCode.map(claimLineStatus),
      inNetworkFlag = mkInNetworkFlag(silver.data),
      serviceQuantity = None, // TODO Cardinal to help identify
      placeOfService = silver.data.PlaceOfServiceCode,
      date = mkDate(silver.data),
      provider = LineProvider(servicing = mkClaimProvider(silver.data.RenderingNPI)),
      procedure = mkProcedure(surrogate, silver),
      amount = mkAmount(silver.data), // TODO Cardinal to help identify
      diagnoses = mkDiagnoses(surrogate, silver.data),
      typesOfService = List.empty // TODO Cardinal to help identify because values for ClaimTypeCode look off
    )

  private def mkDate(silver: ParsedPhysical): ProfessionalDate =
    ProfessionalDate(
      from = silver.ServiceStartDate,
      to = silver.ServiceEndDate,
      paid = silver.PaidDate
    )

  private def mkProfessional(silverHeader: Option[(Surrogate, SilverPhysical)],
                             lines: List[Line]): Option[Professional] =
    for {
      (surrogate, header) <- silverHeader
    } yield {
      Professional(
        claimId = Transform.mkIdentifier(header.data.ClaimNumber),
        memberIdentifier = mkMemberIdentifier(header.patient),
        header = mkHeader(surrogate, header),
        lines = lines
      )
    }

  private def mkHeader(surrogate: Surrogate, header: SilverPhysical): Header =
    Header(
      partnerClaimId = header.data.ClaimNumber,
      lineOfBusiness = mkLineOfBusiness(header.data),
      subLineOfBusiness = None,
      provider = HeaderProvider(
        billing = mkClaimProvider(header.data.BillingNPI),
        referring = mkClaimProvider(header.data.ReferralNPI)
      ),
      diagnoses = mkDiagnoses(surrogate, header.data)
    )
}
