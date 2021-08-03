package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.{UBH, UBHDiagnosis, UBHMedicare}
import cityblock.models.Surrogate
import cityblock.models.connecticare.silver.UBH.ParsedUBH
import cityblock.models.gold.Claims.MemberIdentifier
import cityblock.models.gold.ProfessionalClaim.{
  Header,
  HeaderProvider,
  Line,
  LineProvider,
  Professional,
  ProfessionalDate
}
import cityblock.models.gold.ProviderIdentifier
import com.spotify.scio.values.SCollection
import cityblock.transforms.Transform.Transformer
import cityblock.transforms.connecticare.AmisysClaimKey
import cityblock.utilities.Conversions

case class ProfessionalBehavioralTransformer(
  commercial: SCollection[UBH],
  commercialDiagnoses: SCollection[UBHDiagnosis],
  medicare: SCollection[UBHMedicare],
  medicareDiagnoses: SCollection[UBHDiagnosis]
) extends Transformer[Professional]
    with BehavioralMapping {
  override def transform()(implicit pr: String): SCollection[Professional] = {
    val mappedMedicare: SCollection[UBH] = medicare.map(toCommercial)

    val commercialTransformed: SCollection[Professional] = {
      ProfessionalBehavioralTransformer.pipeline(
        pr,
        commercial,
        "UBH",
        commercialDiagnoses,
        "UBHDiagnosis",
        false
      )
    }

    val medicareTransformed: SCollection[Professional] = {
      ProfessionalBehavioralTransformer.pipeline(
        pr,
        mappedMedicare,
        "UBH_med",
        medicareDiagnoses,
        "UBHDiagnosis_med",
        true
      )
    }

    commercialTransformed.union(medicareTransformed)
  }
}

object ProfessionalBehavioralTransformer extends BehavioralMapping {
  def pipeline(
    project: String,
    claims: SCollection[UBH],
    claimsTable: String,
    ubhDiagnoses: SCollection[UBHDiagnosis],
    ubhDiagnosesTable: String,
    isMedicare: Boolean
  ): SCollection[Professional] = {
    val claimWithSurrogates: SCollection[(Surrogate, UBH)] =
      addSurrogate(project, "silver_claims", claimsTable, claims)

    val diagnoses = ubhDiagnosesByDiagnosisId(project, ubhDiagnosesTable, ubhDiagnoses)
    val firsts = firstSilverLinesByClaimKey(claimWithSurrogates)
    val lines = linesByClaimKey(claimWithSurrogates)

    val headersAndDiagnoses = headersWithDiagnoses(firsts, diagnoses)

    headersAndDiagnoses.leftOuterJoin(lines).map {
      case (_, (((_, headers), diagnosis), maybeLines)) =>
        mkProfessional(
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

  private def mkHeaderProvider(claim: ParsedUBH): HeaderProvider = {
    val billingProvider: Option[ProviderIdentifier] = for {
      providerId <- claim.PRV_NBR
    } yield {
      ProviderIdentifier(
        id = ProviderKey(providerId).uuid.toString,
        specialty = None
      )
    }

    HeaderProvider(
      billing = billingProvider,
      referring = None
    )
  }

  private def mkHeader(
    first: UBH,
    surrogate: Surrogate,
    diagnosis: Iterable[(Surrogate, UBHDiagnosis)],
    isMedicare: Boolean
  ): Header = {
    val claim = first.claim
    Header(
      partnerClaimId = claim.AUDNBR,
      lineOfBusiness = mkLineOfBusiness(isMedicare),
      subLineOfBusiness = mkSubLineOfBusiness(isMedicare),
      provider = mkHeaderProvider(claim),
      diagnoses = mkDiagnoses(claim, surrogate, diagnosis).distinct
    )
  }

  private def mkLineProviderSpecialtyCode(claim: ParsedUBH): Option[String] =
    Conversions.stripLeadingZeros(claim.PRV_SPEC, 2)

  private def mkLineProvider(claim: ParsedUBH): LineProvider = {
    val servicingProvider: Option[ProviderIdentifier] = {
      for {
        providerId: String <- claim.PRV_NBR
      } yield {
        ProviderIdentifier(
          id = ProviderKey(providerId).uuid.toString,
          specialty = mkLineProviderSpecialtyCode(claim)
        )
      }
    }

    LineProvider(
      servicing = servicingProvider
    )
  }

  private def mkLineDate(claim: ParsedUBH): ProfessionalDate =
    ProfessionalDate(
      from = claim.FROM_DOS,
      to = claim.TO_DOS,
      paid = None
    )

  private def mkPlaceOfService(line: ParsedUBH): Option[String] =
    Conversions.stripLeadingZeros(line.POS, 2)

  private def mkLine(surrogate: Surrogate, silver: UBH): Option[Line] = {
    val claim: ParsedUBH = silver.claim
    for {
      lineNumber <- mkLineNumber(claim)
    } yield {
      Line(
        surrogate = surrogate,
        lineNumber = lineNumber,
        cobFlag = None,
        capitatedFlag = None,
        claimLineStatus = mkClaimLineStatus(claim),
        inNetworkFlag = mkInNetworkFlag(claim),
        serviceQuantity = claim.UNITS,
        placeOfService = mkPlaceOfService(claim),
        date = mkLineDate(claim),
        provider = mkLineProvider(claim),
        procedure = mkProcedure(surrogate, claim),
        amount = mkAmount(claim),
        diagnoses = List(),
        typesOfService = mkTypeOfService(claim)
      )
    }
  }

  private def mkMemberIdentifier(headerLine: UBH): MemberIdentifier =
    MemberIdentifier(
      patientId = headerLine.patient.patientId,
      partnerMemberId = headerLine.patient.externalId,
      commonId = headerLine.patient.source.commonId,
      partner = partner
    )

  private def mkProfessional(diagnosis: Iterable[(Surrogate, UBHDiagnosis)],
                             first: (Surrogate, UBH),
                             lines: List[Line],
                             isMedicare: Boolean): Professional = {
    val headerLine = first._2
    val surrogate = first._1

    Professional(
      claimId = AmisysClaimKey(headerLine).uuid.toString,
      memberIdentifier = mkMemberIdentifier(headerLine),
      header = mkHeader(headerLine, surrogate, diagnosis, isMedicare),
      lines = lines
    )
  }
}
