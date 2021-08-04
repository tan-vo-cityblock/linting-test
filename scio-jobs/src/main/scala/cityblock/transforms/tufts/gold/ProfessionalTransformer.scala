package cityblock.transforms.tufts.gold

import cityblock.models.Surrogate
import cityblock.models.TuftsSilverClaims.Medical
import cityblock.models.gold.Claims.ClaimLineStatus
import cityblock.models.gold.ProfessionalClaim.{
  Header,
  HeaderProvider,
  Line,
  LineProvider,
  Professional,
  ProfessionalDate
}
import cityblock.models.gold.ProviderIdentifier
import cityblock.models.tufts.silver.Medical.ParsedMedical
import cityblock.transforms.Transform.Transformer
import cityblock.transforms.tufts.{ClaimKey, ProviderKey}
import cityblock.utilities.Insurance.{LineOfBusiness, SubLineOfBusiness}
import cityblock.utilities.reference.tables.ProviderSpecialtyMappings
import com.spotify.scio.values.SCollection

case class ProfessionalTransformer(
  professional: SCollection[Medical],
  providerSpecialties: SCollection[ProviderSpecialtyMappings]
) extends Transformer[Professional] {
  override def transform()(implicit pr: String): SCollection[Professional] =
    ProfessionalTransformer.pipeline(pr, "Medical", professional, providerSpecialties)
}

object ProfessionalTransformer extends MedicalMapping {
  def pipeline(
    project: String,
    table: String,
    silver: SCollection[Medical],
    providerSpecialities: SCollection[ProviderSpecialtyMappings]
  ): SCollection[Professional] = {
    val claimsWithSurrogates = addSurrogate(project, table, silver)
    val linesWithLineSpecialties =
      linesWithLineSpecialty(claimsWithSurrogates, providerSpecialities)

    val linesToUse = mkCaseClass(linesWithLineSpecialties)

    val lines = dedupeLines(project, table, linesToUse)
    val firstsByClaimKey = firstByClaimKey(linesToUse)

    firstsByClaimKey
      .leftOuterJoin(lines)
      .values
      .map(lines => {
        mkProfessional(lines._1, lines._2.getOrElse(List.empty))
      })
  }

  private def linesByClaimKey(
    silvers: SCollection[LineWithSpecialties]
  ): SCollection[(ClaimKey, Iterable[(Line, Int)])] =
    silvers.map(silver => (ClaimKey(silver.claimLine), mkLine(silver))).groupByKey

  private def dedupeLines(
    project: String,
    table: String,
    claimWithSurrogates: SCollection[LineWithSpecialties]
  ): SCollection[(ClaimKey, List[Line])] = {
    val linesByClaimKeys = linesByClaimKey(claimWithSurrogates)

    linesByClaimKeys.map(linesByClaimKey => {
      val linesByLineNumber = linesByClaimKey._2.groupBy(_._1.lineNumber)
      val sortedLines = linesByLineNumber.values.toList.map(_.toList.sortBy(_._2))
      val onlySortedLines: List[List[Line]] = sortedLines.map(_.map(_._1))
      val outdatedLines: List[List[Line]] =
        onlySortedLines.map(lines => lines.slice(0, lines.size - 1))
      val deniedLines: List[List[Line]] =
        outdatedLines.map(_.map(_.copy(claimLineStatus = Some(ClaimLineStatus.Denied.toString))))
      val paidLines: List[List[Line]] =
        onlySortedLines.map(lines => lines.slice(lines.size - 1, lines.size))
      val allLines: List[Line] = (deniedLines ++ paidLines).flatten
      (linesByClaimKey._1, allLines)
    })
  }

  private def mkDate(line: ParsedMedical): ProfessionalDate =
    ProfessionalDate(
      from = line.Date_of_Service_From,
      to = line.Date_of_Service_To,
      paid = line.Paid_Date
    )

  private def mkProvider(line: ParsedMedical,
                         providerSpecialty: Option[ProviderSpecialtyMappings]): LineProvider = {
    val servicing: Option[ProviderIdentifier] = line.Service_Provider_Number.map(providerNumber => {
      ProviderIdentifier(
        id = ProviderKey(providerNumber).uuid.toString,
        specialty = providerSpecialty.flatMap(_.medicareSpecialty)
      )
    })

    LineProvider(
      servicing = servicing
    )
  }

  private def mkLine(silver: LineWithSpecialties): (Line, Int) = {
    val claimLine: ParsedMedical = silver.claimLine.data
    val line = Line(
      surrogate = silver.surrogate,
      lineNumber = claimLine.Line_Counter,
      cobFlag = mkCobFlag(claimLine),
      capitatedFlag = mkCapitatedFlag(claimLine),
      claimLineStatus = mkClaimLineStatus(claimLine),
      inNetworkFlag = mkInNetworkFlag(claimLine),
      serviceQuantity = mkServiceQuantity(claimLine),
      placeOfService = claimLine.Site_of_Service_on_NSF_CMS_1500_Claims,
      date = mkDate(claimLine),
      provider = mkProvider(claimLine, silver.lineSpecialty),
      procedure = mkProcedure(claimLine, silver.surrogate),
      amount = mkAmount(claimLine),
      diagnoses = List.empty,
      typesOfService = List.empty
    )

    (line, getVersionNumber(claimLine).getOrElse(0))
  }

  def mkHeaderProvider(line: ParsedMedical): HeaderProvider = {
    val billing: Option[ProviderIdentifier] = mkProviderIdentifier(line.Billing_Provider_Number)
    val referring: Option[ProviderIdentifier] = mkProviderIdentifier(line.Referring_Provider_ID)

    HeaderProvider(
      billing = billing,
      referring = referring
    )
  }

  private def mkHeader(surrogate: Surrogate, first: Medical): Header = {
    val headerLine: ParsedMedical = first.data

    Header(
      partnerClaimId = headerLine.Payer_Claim_Control_Number,
      lineOfBusiness = Some(LineOfBusiness.Medicare.toString),
      subLineOfBusiness = Some(SubLineOfBusiness.Dual.toString),
      provider = mkHeaderProvider(headerLine),
      diagnoses = mkDiagnoses(headerLine, surrogate)
    )
  }

  private def mkProfessional(first: LineWithSpecialties, lines: List[Line]): Professional = {
    val headerLine = first.claimLine

    Professional(
      claimId = mkClaimId(headerLine),
      memberIdentifier = mkMemberIdentifier(headerLine),
      header = mkHeader(first.surrogate, headerLine),
      lines = lines
    )
  }
}
