package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.FacetsMedical
import cityblock.models.Surrogate
import cityblock.models.connecticare.silver.FacetsMedical.ParsedFacetsMedical
import cityblock.models.gold.Claims.Procedure
import cityblock.models.gold.{Amount, Constructors, ProviderIdentifier}
import cityblock.models.gold.ProfessionalClaim.{
  Header,
  HeaderProvider,
  Line,
  LineProvider,
  Professional,
  ProfessionalDate
}
import cityblock.models.gold.enums.ProcedureTier
import cityblock.transforms.Transform
import cityblock.transforms.Transform.Transformer
import cityblock.utilities.Conversions
import com.spotify.scio.values.SCollection

case class ProfessionalFacetsTransformer(medical: SCollection[FacetsMedical])
    extends Transformer[Professional] {
  override def transform()(implicit pr: String): SCollection[Professional] =
    ProfessionalFacetsTransformer.pipeline(pr, medical)
}

object ProfessionalFacetsTransformer extends MedicalFacetsMapping {
  def pipeline(
    project: String,
    claimLines: SCollection[FacetsMedical]
  ): SCollection[Professional] = {
    val linesWithSurrogates: SCollection[(Surrogate, FacetsMedical)] =
      addSurrogate(project, "Professional", claimLines)

    val firsts: SCollection[(String, (Surrogate, FacetsMedical))] =
      firstSilverLinesByClaimKey(linesWithSurrogates)

    val lines: SCollection[(String, Iterable[Line])] = linesByClaimKey(linesWithSurrogates)

    firsts.leftOuterJoin(lines).map {
      case (_, (surrogateAndMedical, lines)) => mkProfessional(surrogateAndMedical, lines)
    }
  }

  def firstSilverLinesByClaimKey(
    claims: SCollection[(Surrogate, FacetsMedical)]
  ): SCollection[(String, (Surrogate, FacetsMedical))] =
    claims
      .keyBy(claim => mkFacetsPartnerClaimId(claim._2))
      .reduceByKey(CCIFacetsLineOrdering.min)

  private def linesByClaimKey(
    claimWithSurrogates: SCollection[(Surrogate, FacetsMedical)]
  ): SCollection[(String, Iterable[Line])] =
    claimWithSurrogates
      .map {
        case (surrogate, claim) =>
          val line: Option[Line] = mkLine(surrogate, claim)
          (mkFacetsPartnerClaimId(claim), line)
      }
      .groupByKey
      .map(claim => (claim._1, claim._2.flatten))

  private def mkDate(silver: ParsedFacetsMedical): ProfessionalDate =
    ProfessionalDate(
      to = silver.EXT_BEG_DOS,
      from = silver.EXT_END_DOS,
      paid = None
    )

  private def mkAmount(silver: ParsedFacetsMedical): Amount =
    Constructors.mkAmount(
      allowed = silver.EXT_ALLOWED_AMOUNT_1,
      billed = silver.EXT_BILLED_AMT_1,
      COB = None,
      copay = silver.COPAY,
      deductible = silver.DEDUCTIBLE,
      coinsurance = silver.COINSURANCE,
      planPaid = silver.EXT_PAID_AMT_1
    )

  private def mkServiceQuantity(silver: ParsedFacetsMedical): Option[Int] =
    silver.EXT_UNITS_N_1.flatMap(units => Conversions.safeParse(units, _.toInt))

  private def mkServicingProviderIdentifier(line: ParsedFacetsMedical): Option[ProviderIdentifier] =
    line.EXT_2001_PROVIDER_ID.map(providerId => {
      ProviderIdentifier(
        id = FacetsProviderKey(providerId).uuid.toString,
        specialty = line.EXT_2712_PROV_SPECIALTY
      )
    })

  private def mkProvider(silver: ParsedFacetsMedical): LineProvider =
    LineProvider(servicing = mkServicingProviderIdentifier(silver))

  private def mkProcedure(surrogate: Surrogate, silver: ParsedFacetsMedical): Option[Procedure] =
    silver.EXT_1218_PROC_CODE.map(code => {
      Procedure(
        surrogate = surrogate,
        tier = ProcedureTier.Secondary.name,
        codeset = Transform.determineProcedureCodeset(code),
        code = code,
        modifiers = List(silver.EXT_1219_PROC_MODIFY).flatten
      )
    })

  private def mkLine(surrogate: Surrogate, silver: FacetsMedical): Option[Line] = {
    val claimLine: ParsedFacetsMedical = silver.data
    for {
      lineNumber: Int <- mkLineNumber(claimLine)
    } yield {
      Line(
        surrogate = surrogate,
        lineNumber = lineNumber,
        cobFlag = None,
        capitatedFlag = Some(false),
        claimLineStatus = mkClaimLineStatus(claimLine),
        inNetworkFlag = mkInNetworkFlag(surrogate, claimLine),
        serviceQuantity = mkServiceQuantity(claimLine),
        placeOfService = claimLine.EXT_1207_PLACE_OF_SERVICE,
        date = mkDate(claimLine),
        provider = mkProvider(claimLine),
        procedure = mkProcedure(surrogate, claimLine),
        amount = mkAmount(claimLine),
        diagnoses = List.empty,
        typesOfService = mkTypesOfService(claimLine)
      )
    }
  }

  private def mkHeaderProvider(headerLine: ParsedFacetsMedical): HeaderProvider =
    // Use servicing provider since CCI does not provide billing provider but downstream processes assume its existence.
    HeaderProvider(
      billing = mkServicingProviderIdentifier(headerLine),
      referring = None
    )

  private def mkHeader(surrogate: Surrogate, headerLine: FacetsMedical): Header = {
    val claimLine = headerLine.data
    Header(
      partnerClaimId = mkFacetsPartnerClaimId(headerLine),
      lineOfBusiness = mkLineOfBusiness(surrogate, claimLine.EXT_4332_LOB),
      subLineOfBusiness = mkSubLineOfBusiness(surrogate, claimLine.EXT_4332_LOB),
      provider = mkHeaderProvider(claimLine),
      diagnoses = mkDiagnoses(surrogate, claimLine)
    )
  }

  private def mkProfessional(
    first: (Surrogate, FacetsMedical),
    lines: Option[Iterable[Line]]
  ): Professional = {
    val surrogate = first._1
    val medical = first._2

    Professional(
      claimId = FacetsClaimKey(medical).uuid.toString,
      memberIdentifier = mkMemberIdentifier(medical),
      header = mkHeader(surrogate, medical),
      lines = lines.toList.flatten
    )
  }
}
