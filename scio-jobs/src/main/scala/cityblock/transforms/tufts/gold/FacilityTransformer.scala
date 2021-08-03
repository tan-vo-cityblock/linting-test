package cityblock.transforms.tufts.gold

import cityblock.models.Surrogate
import cityblock.models.TuftsSilverClaims.Medical
import cityblock.models.gold.Claims.ClaimLineStatus
import cityblock.models.gold.FacilityClaim.{
  DRG,
  Date,
  Facility,
  Header,
  HeaderProcedure,
  HeaderProvider,
  Line
}
import cityblock.models.gold.ProviderIdentifier
import cityblock.models.gold.enums.ProcedureTier
import cityblock.models.tufts.silver.Medical.ParsedMedical
import cityblock.transforms.Transform.{CodeSet, DRGCodeSet, Transformer}
import cityblock.transforms.tufts.{ClaimKey, ProviderKey}
import cityblock.utilities.Conversions
import cityblock.utilities.Insurance.{LineOfBusiness, SubLineOfBusiness}
import cityblock.utilities.reference.tables.ProviderSpecialtyMappings
import com.spotify.scio.values.SCollection

case class FacilityTransformer(
  facility: SCollection[Medical],
  providerSpecialties: SCollection[ProviderSpecialtyMappings]
) extends Transformer[Facility] {
  override def transform()(implicit pr: String): SCollection[Facility] =
    FacilityTransformer.pipeline(pr, "Medical", facility, providerSpecialties)
}

object FacilityTransformer extends MedicalMapping {
  def pipeline(
    project: String,
    table: String,
    silver: SCollection[Medical],
    providerSpecialities: SCollection[ProviderSpecialtyMappings]
  ): SCollection[Facility] = {
    val claimsWithSurrogates = addSurrogate(project, table, silver)
    val linesWithLineSpecialties =
      linesWithLineSpecialty(claimsWithSurrogates, providerSpecialities)

    val linesToUse = mkCaseClass(linesWithLineSpecialties)

    val lines = dedupeLines(linesToUse)
    val firstsByClaimKey = firstByClaimKey(linesToUse)

    firstsByClaimKey
      .leftOuterJoin(lines)
      .values
      .map { case (header, maybeLines) => mkFacility(header, maybeLines.getOrElse(List.empty)) }
  }

  private def linesByClaimKey(
    silvers: SCollection[LineWithSpecialties]
  ): SCollection[(ClaimKey, Iterable[(Line, Int)])] =
    silvers.map(silver => (ClaimKey(silver.claimLine), mkLine(silver))).groupByKey

  private def dedupeLines(
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

  private def mkLine(silver: LineWithSpecialties): (Line, Int) = {
    val claimLine: ParsedMedical = silver.claimLine.data
    val line = Line(
      surrogate = silver.surrogate,
      lineNumber = claimLine.Line_Counter,
      revenueCode = claimLine.Revenue_Code,
      cobFlag = mkCobFlag(claimLine),
      capitatedFlag = mkCapitatedFlag(claimLine),
      claimLineStatus = mkClaimLineStatus(claimLine),
      inNetworkFlag = mkInNetworkFlag(claimLine),
      serviceQuantity = mkServiceQuantity(claimLine),
      typesOfService = List.empty,
      procedure = mkProcedure(claimLine, silver.surrogate),
      amount = mkAmount(claimLine)
    )

    (line, getVersionNumber(claimLine).getOrElse(0))
  }

  private def mkTypeOfBill(header: ParsedMedical): Option[String] = {
    val claimTypeOfBill: Option[String] = header.Type_of_Bill_on_Facility_Claims
    claimTypeOfBill.map("0" + _ + "1")
  }

  private def mkDRG(header: ParsedMedical): DRG = {
    def mkCodeset(header: ParsedMedical): Option[String] =
      if (header.DRG.flatMap(Conversions.safeParse(_, _.toInt)).exists(_ < 4)) {
        Some(DRGCodeSet.msDrg.toString)
      } else {
        None
      }

    def mkCode(header: ParsedMedical): Option[String] = {
      for {
        code <- header.DRG
      } yield {
        if (code == "000") {
          None
        } else {
          Some(("000" + code).substring(code.length()))
        }
      }
    }.flatten

    DRG(
      version = header.DRG_Version,
      codeset = mkCodeset(header),
      code = mkCode(header)
    )
  }

  private def mkDate(header: ParsedMedical): Date =
    Date(
      from = header.Date_of_Service_From,
      to = header.Date_of_Service_To,
      admit = header.Admission_Date,
      discharge = header.Discharge_Date,
      paid = header.Paid_Date
    )

  private def mkSecondaryProcedure(procedureCode: String, surrogate: Surrogate): HeaderProcedure =
    HeaderProcedure(
      surrogate = surrogate,
      tier = ProcedureTier.Secondary.name,
      codeset = CodeSet.ICD10Pcs.toString,
      code = procedureCode
    )

  private def mkProcedures(header: ParsedMedical, surrogate: Surrogate): List[HeaderProcedure] = {
    val primaryProcedure = header.ICD_CM_Procedure_Code
      .map(code => {
        HeaderProcedure(
          surrogate = surrogate,
          tier = ProcedureTier.Principal.name,
          codeset = CodeSet.ICD10Pcs.toString,
          code = code
        )
      })
      .toList

    val secondaryCodes = List(
      header.Other_ICD_CM_Procedure_Code_1,
      header.Other_ICD_CM_Procedure_Code_2,
      header.Other_ICD_CM_Procedure_Code_3,
      header.Other_ICD_CM_Procedure_Code_4,
      header.Other_ICD_CM_Procedure_Code_5,
      header.Other_ICD_CM_Procedure_Code_6
    ).flatten

    val secondaryProcedures = secondaryCodes.map(mkSecondaryProcedure(_, surrogate))

    primaryProcedure ++ secondaryProcedures
  }

  def mkHeaderProvider(
    line: ParsedMedical,
    providerSpecialty: Option[ProviderSpecialtyMappings]
  ): HeaderProvider = {
    val billing: Option[ProviderIdentifier] = mkProviderIdentifier(line.Billing_Provider_Number)
    val referring: Option[ProviderIdentifier] = mkProviderIdentifier(line.Referring_Provider_ID)
    val servicing: Option[ProviderIdentifier] = line.Service_Provider_Number.map(id => {
      ProviderIdentifier(
        id = ProviderKey(id).uuid.toString,
        specialty = providerSpecialty.flatMap(_.medicareSpecialty)
      )
    })

    HeaderProvider(
      billing = billing,
      referring = referring,
      servicing = servicing,
      operating = None
    )
  }

  private def mkHeader(
    surrogate: Surrogate,
    first: Medical,
    providerSpecialty: Option[ProviderSpecialtyMappings]
  ): Header = {
    val headerLine: ParsedMedical = first.data

    Header(
      partnerClaimId = headerLine.Payer_Claim_Control_Number,
      typeOfBill = mkTypeOfBill(headerLine),
      admissionType = headerLine.Admission_Type,
      admissionSource = headerLine.Admission_Source,
      dischargeStatus = headerLine.Discharge_Status,
      lineOfBusiness = Some(LineOfBusiness.Medicare.toString),
      subLineOfBusiness = Some(SubLineOfBusiness.Dual.toString),
      drg = mkDRG(headerLine),
      provider = mkHeaderProvider(headerLine, providerSpecialty),
      diagnoses = mkDiagnoses(headerLine, surrogate),
      procedures = mkProcedures(headerLine, surrogate),
      date = mkDate(headerLine)
    )
  }

  private def mkFacility(first: LineWithSpecialties, lines: List[Line]): Facility = {
    val headerLine = first.claimLine

    Facility(
      claimId = mkClaimId(headerLine),
      memberIdentifier = mkMemberIdentifier(headerLine),
      header = mkHeader(first.surrogate, headerLine, first.lineSpecialty),
      lines = lines
    )
  }
}
