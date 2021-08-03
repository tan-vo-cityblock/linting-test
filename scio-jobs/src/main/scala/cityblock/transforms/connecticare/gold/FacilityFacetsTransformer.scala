package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.{
  FacetsMedical,
  FacetsMedicalRevenueCodes,
  FacetsMedicalRevenueUnits
}
import cityblock.models.Surrogate
import cityblock.models.connecticare.silver.FacetsMedical.ParsedFacetsMedical
import cityblock.models.connecticare.silver.FacetsMedicalRevenueCodes.ParsedFacetsRevenueCodes
import cityblock.models.connecticare.silver.FacetsMedicalRevenueUnits.ParsedFacetsRevenueUnits
import cityblock.models.gold.{Amount, Constructors, ProviderIdentifier}
import cityblock.models.gold.FacilityClaim.{
  DRG,
  Date,
  Facility,
  Header,
  HeaderProcedure,
  HeaderProvider,
  Line
}
import cityblock.models.gold.enums.{DiagnosisTier, ProcedureTier}
import cityblock.transforms.Transform.{CodeSet, DRGCodeSet, Transformer}
import cityblock.utilities.{Conversions, Strings}
import com.spotify.scio.values.SCollection
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

case class FacilityFacetsTransformer(medical: SCollection[FacetsMedical],
                                     revenueCodes: SCollection[FacetsMedicalRevenueCodes],
                                     revenueUnits: SCollection[FacetsMedicalRevenueUnits])
    extends Transformer[Facility] {
  override def transform()(implicit pr: String): SCollection[Facility] =
    FacilityFacetsTransformer.pipeline(pr, medical, revenueCodes, revenueUnits)
}

object FacilityFacetsTransformer extends MedicalFacetsMapping {
  def pipeline(
    project: String,
    claimLines: SCollection[FacetsMedical],
    claimLinesRevenueCodes: SCollection[FacetsMedicalRevenueCodes],
    claimLinesRevenueUnits: SCollection[FacetsMedicalRevenueUnits]
  ): SCollection[Facility] = {
    val linesWithSurrogates: SCollection[(Surrogate, FacetsMedical)] =
      addSurrogate(project, "Facility", claimLines)

    val medical: SCollection[(String, (Surrogate, FacetsMedical))] =
      linesWithSurrogates.keyBy(_._2.data.EXT_1001_CLAIM_NUMBER)

    val lines: SCollection[(String, Iterable[Line])] =
      linesByClaimKey(linesWithSurrogates, claimLinesRevenueCodes, claimLinesRevenueUnits)

    medical.leftOuterJoin(lines).map {
      case (_, (surrogateAndMedical, lines)) =>
        mkProfessional(surrogateAndMedical, lines)
    }
  }

  private def linesByClaimKey(
    claimWithSurrogates: SCollection[(Surrogate, FacetsMedical)],
    claimLinesRevenueCodes: SCollection[FacetsMedicalRevenueCodes],
    claimLinesRevenueUnits: SCollection[FacetsMedicalRevenueUnits]
  ): SCollection[(String, Iterable[Line])] = {
    val keyedClaims = claimWithSurrogates.keyBy(_._2.data.EXT_1001_CLAIM_NUMBER)
    val keyedRevenueCodes = claimLinesRevenueCodes.keyBy(_.data.EXT_1001_CLAIM_NUMBER)
    val keyedRevenueUnits = claimLinesRevenueUnits.keyBy(_.data.EXT_1001_CLAIM_NUMBER)

    val claimsJoinedWithRevenueData = keyedClaims
      .join(keyedRevenueCodes)
      .join(keyedRevenueUnits)

    claimsJoinedWithRevenueData
      .map {
        case (claimId, (((surrogate, claim), revenueCodes), revenueUnits)) =>
          val lines: List[Line] = mkLine(surrogate, claim, revenueCodes, revenueUnits)
          (claimId, lines)
      }
  }

  private def mkProfessional(
    first: (Surrogate, FacetsMedical),
    lines: Option[Iterable[Line]]
  ): Facility = {
    val surrogate = first._1
    val medical = first._2

    Facility(
      claimId = FacetsClaimKey(medical).uuid.toString,
      memberIdentifier = mkMemberIdentifier(medical),
      header = mkHeader(surrogate, medical),
      lines = lines.toList.flatten
    )
  }

  private def mkLine(surrogate: Surrogate,
                     claimLine: FacetsMedical,
                     revenueCodes: FacetsMedicalRevenueCodes,
                     revenueUnits: FacetsMedicalRevenueUnits): List[Line] = {
    val line: ParsedFacetsMedical = claimLine.data

    (for {
      lineNumber: Int <- 1 to 99
    } yield {
      val revenueCode = getRevenueCode(lineNumber, revenueCodes.data)

      if (revenueCode.isDefined && !revenueCode.contains("0001")) {
        Some(
          Line(
            surrogate = surrogate,
            lineNumber = lineNumber,
            revenueCode = getRevenueCode(lineNumber, revenueCodes.data),
            cobFlag = None,
            capitatedFlag = mkCapitatedFlag(line),
            claimLineStatus = mkClaimLineStatus(line),
            inNetworkFlag = mkInNetworkFlag(surrogate, line),
            serviceQuantity = getServiceQuantity(lineNumber, revenueUnits.data),
            typesOfService = mkTypesOfService(line),
            procedure = None,
            amount = mkAmount(lineNumber, line)
          ))
      } else {
        None
      }
    }).toList.flatten
  }

  private def mkCapitatedFlag(claimLine: ParsedFacetsMedical): Option[Boolean] =
    claimLine.EXT_FFS_OR_CAP_IND_1.map {
      case "F" => false
      case _   => true
    }

  private def getRevenueCode(lineNumber: Int,
                             revenueCodes: ParsedFacetsRevenueCodes): Option[String] = {
    val field = s"EXT_1714_REVENUE_CODE$lineNumber"
    val params = Conversions.getCaseClassParams(revenueCodes)
    Conversions.getValueForField(field, params)
  }

  private def getServiceQuantity(lineNumber: Int,
                                 revenueUnits: ParsedFacetsRevenueUnits): Option[Int] = {
    val serviceQuantity = s"EXT_REVENUE_UNITS$lineNumber"
    val params = Conversions.getCaseClassParams(revenueUnits)
    Conversions.getValueForField(serviceQuantity, params).flatMap(Conversions.safeParse(_, _.toInt))
  }

  private def mkAmount(lineNumber: Int, line: ParsedFacetsMedical): Amount =
    if (lineNumber == 1) {
      Constructors.mkAmount(
        allowed = Some(line.EXT_ALLOWED_AMOUNT_1.getOrElse("0")),
        billed = Some(line.EXT_BILLED_AMT_1.getOrElse("0")),
        COB = None,
        copay = Some(line.COPAY.getOrElse("0")),
        deductible = Some(line.DEDUCTIBLE.getOrElse("0")),
        coinsurance = Some(line.COINSURANCE.getOrElse("0")),
        planPaid = Some(line.EXT_PAID_AMT_1.getOrElse("0"))
      )
    } else {
      Constructors.mkAmount(
        allowed = None,
        billed = None,
        COB = None,
        copay = None,
        deductible = None,
        coinsurance = None,
        planPaid = None
      )
    }

  private def mkHeader(surrogate: Surrogate, headerLine: FacetsMedical): Header = {
    val line: ParsedFacetsMedical = headerLine.data

    Header(
      partnerClaimId = line.EXT_1001_CLAIM_NUMBER,
      typeOfBill = line.EXT_1700_BILL_TYPE,
      admissionType = line.EXT_1531_ADMIT_TYPE,
      admissionSource = line.EXT_1721_ADMIT_SOURCE,
      dischargeStatus = line.EXT_1704_DISCHARGE_STATUS,
      lineOfBusiness = mkLineOfBusiness(surrogate, line.EXT_4332_LOB),
      subLineOfBusiness = mkSubLineOfBusiness(surrogate, line.EXT_4332_LOB),
      drg = mkDrg(line),
      provider = mkProvider(line),
      diagnoses = mkDiagnoses(surrogate, line),
      procedures = mkProcedures(surrogate, line),
      date = mkDate(line)
    )
  }

  private def mkDrg(line: ParsedFacetsMedical): DRG = {
    val rawDrg = line.EXT_1653_DRG

    val codeAndCodeSet: Option[(String, Option[String])] =
      for {
        drg: String <- rawDrg
      } yield {
        if (drg.length == 4) {
          val code = rawDrg.map(drg => ("0000" + drg).substring(drg.length()))
          val cleanedCode: Option[String] =
            if (code.exists(_.charAt(0) == '0')) code.map(_.substring(1, 4)) else code
          (DRGCodeSet.aprDrg.toString, cleanedCode)
        } else {
          val code = rawDrg.map(Strings.zeroPad(_, 3))
          (DRGCodeSet.msDrg.toString, code)
        }
      }

    DRG(
      version = None,
      codeset = codeAndCodeSet.map(_._1),
      code = codeAndCodeSet.flatMap(_._2)
    )
  }

  private def mkProvider(line: ParsedFacetsMedical): HeaderProvider = {
    val providerId: Option[ProviderIdentifier] = line.EXT_2001_PROVIDER_ID.map(id => {
      ProviderIdentifier(
        id = FacetsProviderKey(id).uuid.toString,
        specialty = line.EXT_2712_PROV_SPECIALTY
      )
    })

    HeaderProvider(
      // Use servicing the same ID for servicing and billing provider since CCI does not provide both and downstream
      // dbt processes expect both to exist.
      billing = providerId,
      referring = None,
      servicing = providerId,
      operating = None
    )
  }

  private def mkProcedures(
    surrogate: Surrogate,
    headerLine: ParsedFacetsMedical
  ): List[HeaderProcedure] = {
    val headerProcedure: Option[HeaderProcedure] =
      headerLine.EXT_1506_PRIN_PROC_CODE.map(code => {
        HeaderProcedure(
          surrogate = surrogate,
          tier = ProcedureTier.Principal.name,
          codeset = mkProcedureCodeset(headerLine),
          code = code
        )
      })

    val secondaryProcedureCodes: List[String] = List(
      headerLine.EXT_1506_ICD9_PROC_CODE1,
      headerLine.EXT_1506_ICD9_PROC_CODE2,
      headerLine.EXT_1506_ICD9_PROC_CODE3,
      headerLine.EXT_1506_ICD9_PROC_CODE4,
      headerLine.EXT_1506_ICD9_PROC_CODE5,
      headerLine.EXT_1506_ICD9_PROC_CODE6,
      headerLine.EXT_1506_ICD9_PROC_CODE7,
      headerLine.EXT_1506_ICD9_PROC_CODE8,
      headerLine.EXT_1506_ICD9_PROC_CODE9,
      headerLine.EXT_1506_ICD9_PROC_CODE10,
      headerLine.EXT_1506_ICD9_PROC_CODE11,
      headerLine.EXT_1506_ICD9_PROC_CODE12
    ).flatten

    val secondaryProcedures: List[HeaderProcedure] = secondaryProcedureCodes.map(code => {
      HeaderProcedure(
        surrogate = surrogate,
        tier = DiagnosisTier.Secondary.name,
        codeset = mkProcedureCodeset(headerLine),
        code = code
      )
    })

    (headerProcedure ++ secondaryProcedures).toList
  }

  private def mkProcedureCodeset(headerLine: ParsedFacetsMedical): String =
    headerLine.ICD_INDICATOR
      .map {
        case "9" => CodeSet.ICD9Pcs.toString
        case _   => CodeSet.ICD10Pcs.toString
      }
      .getOrElse(CodeSet.ICD10Pcs.toString)

  private def mkDate(line: ParsedFacetsMedical): Date = {
    val paidDateFormat = DateTimeFormat.forPattern("yyyyMMdd")
    val paidDate: Option[LocalDate] =
      line.EXT_PAYMT_DATE_2
        .orElse(line.EXT_PAYMT_DATE_1)
        .map(paid => LocalDate.parse(paid, paidDateFormat))

    Date(
      from = line.EXT_BEG_DOS,
      to = line.EXT_END_DOS,
      admit = line.EXT_1503_ADMIT_DT,
      discharge = line.EXT_DISCHARGE_DT,
      paid = paidDate
    )
  }
}
