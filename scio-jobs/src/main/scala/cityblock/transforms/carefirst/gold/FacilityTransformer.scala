package cityblock.transforms.carefirst.gold

import cityblock.models.CarefirstSilverClaims.SilverFacility
import cityblock.models.Surrogate
import cityblock.models.carefirst.silver.Facility.ParsedFacility
import cityblock.models.gold.Claims.{Diagnosis, MemberIdentifier, Procedure}
import cityblock.models.gold.FacilityClaim._
import cityblock.models.gold.enums.{DiagnosisTier, ProcedureTier}
import cityblock.models.gold.{Amount, Constructors, FacilityClaim, ProviderIdentifier}
import cityblock.transforms.Transform
import cityblock.transforms.Transform.{CodeSet, Transformer}
import cityblock.utilities.Conversions
import cityblock.utilities.Insurance.LineOfBusiness
import com.spotify.scio.util.MultiJoin
import com.spotify.scio.values.SCollection

case class FacilityTransformer(facility: SCollection[SilverFacility],
                               diagnosisIndex: SCollection[(ClaimKey, Iterable[Diagnosis])],
                               procedureIndex: SCollection[(ClaimKey, Iterable[Procedure])])
    extends Transformer[Facility] {
  override def transform()(implicit pr: String): SCollection[Facility] =
    FacilityTransformer.pipeline(pr, facility, diagnosisIndex, procedureIndex)
}

object FacilityTransformer extends MedicalMapping with CarefirstLineOrdering[SilverFacility] {
  override def lineNumber(silver: SilverFacility): Option[Int] =
    silver.data.LINE_NUM.flatMap(Conversions.safeParse(_, _.toInt))

  def pipeline(
    project: String,
    facility: SCollection[SilverFacility],
    diagnosisIndex: SCollection[(ClaimKey, Iterable[Diagnosis])],
    procedureIndex: SCollection[(ClaimKey, Iterable[Procedure])]): SCollection[Facility] = {
    val keyedFirstFacility = firstSilverLinesByClaimKey(facility)

    MultiJoin
      .left(
        keyedFirstFacility,
        diagnosisIndex,
        procedureIndex,
        linesbyClaimKey(project, facility)
      )
      .flatMap {
        case (claimKey, (silver, dxs, pxs, Some(lines))) =>
          val silverSurrogate =
            Surrogate(silver.identifier.surrogateId, project, "silver_claims", "facility")
          val firstDx = mkFirstLineDiagnosis(silverSurrogate, silver)
          val firstPx = mkFirstLineProcedure(silverSurrogate, silver)
          val combinedDiagnoses = (dxs.getOrElse(Iterable()) ++ firstDx)
            .map(diagnosis => {
              diagnosis.copy(code = cleanDiagnosisCode(diagnosis.code))
            })
          val diagnoses = Transform
            .uniqueDiagnoses(combinedDiagnoses)
            .toList

          Some(
            mkFacility(claimKey,
                       silver,
                       diagnoses,
                       (pxs.getOrElse(Iterable()) ++ firstPx).toList,
                       lines.toList))
        case _ => None
      }
  }

  private def mkFirstLineDiagnosis(surrogate: Surrogate, first: SilverFacility): Option[Diagnosis] =
    first.data.ICD_DIAG_01.map { code =>
      Diagnosis(
        surrogate = surrogate,
        tier = DiagnosisTier.Principal.name,
        codeset = CodeSet.ICD10Cm.toString,
        code = cleanDiagnosisCode(code)
      )
    }

  private def mkFirstLineProcedure(surrogate: Surrogate, first: SilverFacility): Option[Procedure] =
    first.data.ICD_PROC_01.map { code =>
      Procedure(
        surrogate = surrogate,
        tier = ProcedureTier.Principal.name,
        codeset = CodeSet.ICD10Pcs.toString,
        code = cleanDiagnosisCode(code),
        modifiers = List()
      )
    }

  private def firstSilverLinesByClaimKey(
    facility: SCollection[SilverFacility]): SCollection[(ClaimKey, SilverFacility)] =
    facility
      .keyBy(ClaimKey(_))
      .reduceByKey(CarefirstLineOrdering.min)

  private def linesbyClaimKey(
    project: String,
    silver: SCollection[SilverFacility]): SCollection[(ClaimKey, Iterable[Line])] =
    silver
      .map { f =>
        val (surrogate, _) =
          Transform.addSurrogate(project, "silver_claims", "facility", f)(_.identifier.surrogateId)
        (ClaimKey(f), mkLine(surrogate, f))
      }
      .groupByKey
      .map(grouped => (grouped._1, grouped._2.flatten))

  private def mkLine(surrogate: Surrogate, silver: SilverFacility): Option[Line] =
    for {
      lineNumber <- lineNumber(silver)
    } yield {
      Line(
        surrogate = surrogate,
        lineNumber = lineNumber,
        revenueCode = silver.data.REV_CODE,
        cobFlag = silver.data.COB_IND,
        capitatedFlag = silver.data.CAP_IND,
        claimLineStatus = silver.data.CLAIM_LINE_STATUS.map(claimLineStatus),
        inNetworkFlag = silver.data.CLAIM_IN_NETWORK.flatMap(Conversions.safeParse(_, _.toBoolean)),
        serviceQuantity = silver.data.LOS.flatMap(Conversions.safeParse(_, _.toInt)),
        typesOfService = List.empty,
        procedure = mkProcedure(surrogate, silver),
        amount = mkAmount(silver)
      )
    }

  private def mkProcedure(surrogate: Surrogate, silver: SilverFacility): Option[Procedure] = {
    val tier = if (isFirstLine(silver.data.LINE_NUM)) {
      ProcedureTier.Principal
    } else {
      ProcedureTier.Secondary
    }

    Transform.mkProcedure(
      surrogate,
      silver,
      tier,
      (f: SilverFacility) => f.data.PROC_CODE,
      (f: SilverFacility) => (f.data.CPT_MOD_1 ++ f.data.CPT_MOD_2).toList
    )
  }

  private def mkAmount(silver: SilverFacility): Amount =
    Constructors.mkAmount(
      allowed = silver.data.AMT_ALLOWED,
      billed = silver.data.AMT_BILLED,
      COB = silver.data.AMT_COB,
      copay = silver.data.AMT_COPAY,
      deductible = silver.data.AMT_DEDUCT,
      coinsurance = silver.data.AMT_COINS,
      planPaid = silver.data.AMT_PAID
    )

  private def mkFacility(claimKey: ClaimKey,
                         silver: SilverFacility,
                         diagnoses: List[Diagnosis],
                         procedures: List[Procedure],
                         lines: List[Line]): Facility =
    Facility(
      claimId = claimKey.uuid.toString,
      memberIdentifier = MemberIdentifier(
        commonId = silver.patient.source.commonId,
        partnerMemberId = silver.patient.externalId,
        patientId = silver.patient.patientId,
        partner = partner
      ),
      header = Header(
        partnerClaimId = silver.data.CLAIM_NUM,
        typeOfBill = silver.data.UB_BILL_TYPE.map("0" + _),
        admissionType =
          silver.data.ADM_TYPE.flatMap(Conversions.safeParse(_, _.toInt).map(_.toString)),
        admissionSource = silver.data.ADM_SOURCE,
        dischargeStatus = silver.data.DIS_STAT,
        lineOfBusiness = Some(LineOfBusiness.Medicaid.toString),
        subLineOfBusiness = Some("Supplemental Security Income"),
        drg = DRG(
          version = silver.data.DRG_VERSION,
          codeset = None,
          code = silver.data.DRG
        ),
        provider = FacilityClaim.HeaderProvider(
          billing = mkBillingProvider(silver.data),
          referring = mkReferringProvider(silver.data),
          servicing = mkServicingProvider(silver.data),
          operating = None
        ),
        diagnoses = Transform.uniqueDiagnoses(diagnoses).toList,
        procedures = Transform.uniqueProcedures(procedures).toList,
        date = mkDate(silver.data)
      ),
      lines = lines
    )

  private def mkBillingProvider(data: ParsedFacility): Option[ProviderIdentifier] =
    for (id <- data.BILL_PROV_ID)
      yield
        ProviderIdentifier(
          id = ProviderKey(id).uuid.toString,
          specialty = None
        )

  private def mkReferringProvider(data: ParsedFacility): Option[ProviderIdentifier] =
    for (id <- data.REF_PROV_ID)
      yield
        ProviderIdentifier(
          id = ProviderKey(id).uuid.toString,
          specialty = None
        )

  private def mkServicingProvider(data: ParsedFacility): Option[ProviderIdentifier] =
    for (id <- data.ATT_PROV_ID)
      yield
        ProviderIdentifier(
          id = ProviderKey(id).uuid.toString,
          specialty = data.ATT_PROV_SPEC
        )

  private def mkDate(data: ParsedFacility): Date =
    Date(
      from = data.FROM_DATE,
      to = data.TO_DATE,
      admit = data.ADM_DATE,
      discharge = data.DIS_DATE,
      paid = data.PAID_DATE
    )
}
