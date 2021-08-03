package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.{UBH, UBHDiagnosis, UBHMedicare}
import cityblock.models.Surrogate
import cityblock.models.connecticare.silver.UBH.ParsedUBH
import cityblock.models.gold.Claims.MemberIdentifier
import cityblock.models.gold.FacilityClaim._
import cityblock.models.gold.ProviderIdentifier
import cityblock.models.gold.enums.ProcedureTier
import cityblock.transforms.Transform.{CodeSet, DRGCodeSet, Transformer}
import cityblock.transforms.connecticare.AmisysClaimKey
import com.spotify.scio.values.SCollection

case class FacilityBehavioralTransformer(
  commercial: SCollection[UBH],
  commercialDiagnoses: SCollection[UBHDiagnosis],
  medicare: SCollection[UBHMedicare],
  medicareDiagnoses: SCollection[UBHDiagnosis],
) extends Transformer[Facility] {
  override def transform()(implicit pr: String): SCollection[Facility] = {
    val mappedMedicare: SCollection[UBH] = medicare.map(toCommercial)

    val commercialTransformed: SCollection[Facility] = {
      FacilityBehavioralTransformer.pipeline(
        pr,
        commercial,
        "UBH",
        commercialDiagnoses,
        "UBHDiagnosis",
        false,
      )
    }

    val medicareTransformed: SCollection[Facility] = {
      FacilityBehavioralTransformer.pipeline(
        pr,
        mappedMedicare,
        "UBH_med",
        medicareDiagnoses,
        "UBHDiagnosis_med",
        true,
      )
    }

    commercialTransformed.union(medicareTransformed)
  }
}

object FacilityBehavioralTransformer extends BehavioralMapping {
  def pipeline(
    project: String,
    claims: SCollection[UBH],
    claimsTable: String,
    ubhDiagnoses: SCollection[UBHDiagnosis],
    ubhDiagnosesTable: String,
    isMedicare: Boolean,
  ): SCollection[Facility] = {
    val claimWithSurrogates: SCollection[(Surrogate, UBH)] =
      addSurrogate(project, "silver_claims", claimsTable, claims)

    val diagnoses = ubhDiagnosesByDiagnosisId(project, ubhDiagnosesTable, ubhDiagnoses)
    val firsts = firstSilverLinesByClaimKey(claimWithSurrogates)
    val lines = linesByClaimKey(claimWithSurrogates)

    val headersAndDiagnoses = headersWithDiagnoses(firsts, diagnoses)

    headersAndDiagnoses.leftOuterJoin(lines).map {
      case (_, (((_, headers), diagnosis), maybeLines)) =>
        mkFacility(
          diagnosis,
          headers,
          maybeLines.getOrElse(Iterable()).toList,
          isMedicare
        )
    }
  }

  private def linesByClaimKey(
    claimWithSurrogates: SCollection[(Surrogate, UBH)]
  ): SCollection[(ClaimKey, Iterable[Line])] =
    claimWithSurrogates
      .flatMap({
        case (surrogate, claim) =>
          for (line <- mkLine(surrogate, claim)) yield (ClaimKey(claim), line)
      })
      .groupByKey

  private[gold] def mkFacility(
    ubhDiagnoses: Iterable[(Surrogate, UBHDiagnosis)],
    first: (Surrogate, UBH),
    lines: List[Line],
    isMedicare: Boolean
  ): Facility = {
    val headerLine = first._2
    val surrogate = first._1
    Facility(
      claimId = AmisysClaimKey(headerLine).uuid.toString,
      memberIdentifier = MemberIdentifier(
        patientId = headerLine.patient.patientId,
        partnerMemberId = headerLine.patient.externalId,
        commonId = headerLine.patient.source.commonId,
        partner = partner
      ),
      header = mkHeader(headerLine.claim, surrogate, ubhDiagnoses, isMedicare),
      lines = lines
    )
  }

  private def mkDrg(claim: ParsedUBH): DRG = {
    val rawDrg: Option[String] = claim.DRG
    val code: Option[String] = {
      rawDrg.map(drg => ("000" + drg).substring(drg.length()))
    }
    val codeset: String = {
      if (rawDrg.map(_.length).contains(4)) {
        DRGCodeSet.aprDrg.toString
      } else {
        DRGCodeSet.msDrg.toString
      }
    }

    DRG(
      version = None,
      codeset = Some(codeset),
      code = code
    )
  }

  private def mkDate(header: ParsedUBH): Date = {
    val toDate = header.TO_DOS
    val admitDate = header.ADMIT_DATE_STAGE

    Date(
      from = header.FROM_DOS,
      to = toDate,
      admit = admitDate,
      discharge = if (admitDate.isDefined) toDate else None,
      paid = None
    )
  }

  // NOTE: Using the same ID for both billing and servicing provider since we only get one provider "number" (CCI ID)
  // on behavioral claims (although two NPIs, one for each) and we build our Cityblock IDs off of provider number.
  private def mkProvider(first: ParsedUBH): HeaderProvider =
    HeaderProvider(
      billing = first.PRV_NBR.map(id => {
        ProviderIdentifier(
          id = ProviderKey(id).uuid.toString,
          specialty = None
        )
      }),
      referring = None,
      servicing = first.PRV_NBR.map(id => {
        ProviderIdentifier(
          id = ProviderKey(id).uuid.toString,
          specialty = first.PRV_SPEC
        )
      }),
      operating = None
    )

  private def mkSecondaryProcedure(
    procedure: Option[String],
    surrogate: Surrogate,
    codeset: String
  ): Option[HeaderProcedure] =
    procedure.map(proc => {
      HeaderProcedure(
        surrogate = surrogate,
        tier = ProcedureTier.Secondary.name,
        codeset = codeset,
        code = proc
      )
    })

  private def mkProcedures(first: ParsedUBH, surrogate: Surrogate): Iterable[HeaderProcedure] = {
    val codeset: String = CodeSet.ICD9Pcs.toString

    first.ICD9_PROC_ONE.map(
      proc =>
        HeaderProcedure(
          surrogate = surrogate,
          tier = ProcedureTier.Principal.name,
          codeset = codeset,
          code = proc
      )) ++ mkSecondaryProcedure(first.ICD9_PROC_TWO, surrogate, codeset) ++
      mkSecondaryProcedure(first.ICD9_PROC_THREE, surrogate, codeset) ++
      mkSecondaryProcedure(first.ICD9_PROC_FOUR, surrogate, codeset) ++
      mkSecondaryProcedure(first.ICD9_PROC_FIVE, surrogate, codeset) ++
      mkSecondaryProcedure(first.ICD9_PROC_SIX, surrogate, codeset)
  }

  def mkTypeOfBill(header: ParsedUBH): Option[String] =
    header.BILL.map(typeOfBill => ("0000" + typeOfBill).substring(typeOfBill.length()))

  private def mkHeader(
    first: ParsedUBH,
    claimSurrogate: Surrogate,
    diagnoses: Iterable[(Surrogate, UBHDiagnosis)],
    isMedicare: Boolean
  ): Header =
    Header(
      partnerClaimId = first.AUDNBR,
      typeOfBill = mkTypeOfBill(first),
      admissionType = first.ADMIT_TYPE,
      admissionSource = first.ADMIT_SRC,
      dischargeStatus = first.STATUS,
      lineOfBusiness = mkLineOfBusiness(isMedicare),
      subLineOfBusiness = mkSubLineOfBusiness(isMedicare),
      drg = mkDrg(first),
      provider = mkProvider(first),
      diagnoses = mkDiagnoses(first, claimSurrogate, diagnoses),
      procedures = mkProcedures(first, claimSurrogate).toList,
      date = mkDate(first)
    )

  private def mkLine(surrogate: Surrogate, silver: UBH): Option[Line] = {
    val line: ParsedUBH = silver.claim
    for {
      lineNumber: Int <- mkLineNumber(line)
    } yield {
      Line(
        surrogate = surrogate,
        lineNumber = lineNumber,
        revenueCode = line.REV_CD,
        cobFlag = None,
        capitatedFlag = None,
        claimLineStatus = mkClaimLineStatus(line),
        inNetworkFlag = mkInNetworkFlag(line),
        serviceQuantity = line.UNITS,
        typesOfService = mkTypeOfService(line),
        procedure = mkProcedure(surrogate, line),
        amount = mkAmount(line)
      )
    }
  }
}
