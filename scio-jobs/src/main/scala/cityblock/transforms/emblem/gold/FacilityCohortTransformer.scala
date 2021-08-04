package cityblock.transforms.emblem.gold

import cityblock.models.EmblemSilverClaims.SilverFacilityClaimCohort
import cityblock.models.Surrogate
import cityblock.models.gold.Claims._
import cityblock.models.gold.FacilityClaim._
import cityblock.models.gold.enums.{DiagnosisTier, ProcedureTier}
import cityblock.models.gold.{
  Amount,
  Constructors,
  FacilityClaim,
  ProviderIdentifier,
  TypeOfService
}
import cityblock.transforms.Transform
import cityblock.transforms.Transform.{CodeSet, Transformer}
import cityblock.utilities.emblem.Insurance
import com.spotify.scio.util.MultiJoin
import com.spotify.scio.values.{SCollection, SideInput}
import cityblock.utilities.Conversions
import cityblock.utilities.reference.tables

case class FacilityCohortTransformer(
  facility: SCollection[SilverFacilityClaimCohort],
  mappedLOB: SideInput[Map[String, tables.LineOfBusiness]],
  diagnosisIndex: SCollection[(ClaimKey, Iterable[Diagnosis])],
  procedureIndex: SCollection[(ClaimKey, Iterable[Procedure])],
) extends Transformer[Facility] {
  override def transform()(implicit pr: String): SCollection[Facility] =
    FacilityCohortTransformer.pipeline(pr,
                                       "facility",
                                       facility,
                                       mappedLOB,
                                       diagnosisIndex,
                                       procedureIndex)
}

object FacilityCohortTransformer extends EmblemLineOrdering[SilverFacilityClaimCohort] {
  override def svLine(s: SilverFacilityClaimCohort): Option[Int] =
    s.claim.SV_LINE

  private def pipeline(
    project: String,
    table: String,
    silver: SCollection[SilverFacilityClaimCohort],
    mappedLOB: SideInput[Map[String, tables.LineOfBusiness]],
    diagnosisIndex: SCollection[(ClaimKey, Iterable[Diagnosis])],
    procedureIndex: SCollection[(ClaimKey, Iterable[Procedure])]): SCollection[Facility] =
    MultiJoin
      .left(
        firstSilverLinesByClaimKey(silver),
        diagnosisIndex,
        procedureIndex,
        linesByClaimKey(project, table, silver)
      )
      .withSideInputs(mappedLOB)
      .map {
        case ((_, (first, diagnoses, procedures, lines)), ctx) =>
          val firstSurrogate =
            Surrogate(first.identifier.surrogateId, project, "silver_claims", table)
          val firstDx = mkFirstLineDiagnosis(firstSurrogate, first)
          val firstPx = mkFirstLineProcedure(firstSurrogate, first)
          mkFacility(
            first,
            ctx(mappedLOB),
            (diagnoses.getOrElse(Iterable()) ++ firstDx).toList,
            (procedures.getOrElse(Iterable()) ++ firstPx).toList,
            lines.getOrElse(Iterable()).toList
          )
      }
      .toSCollection

  private def firstSilverLinesByClaimKey(silver: SCollection[SilverFacilityClaimCohort])
    : SCollection[(ClaimKey, SilverFacilityClaimCohort)] =
    silver
      .keyBy(ClaimKey(_))
      .reduceByKey(EmblemLineOrdering.min)

  private def linesByClaimKey(
    project: String,
    table: String,
    silver: SCollection[SilverFacilityClaimCohort]): SCollection[(ClaimKey, Iterable[Line])] =
    silver.map { s =>
      val (surrogate, _) =
        Transform.addSurrogate(project, "silver_claims", table, s)(_.identifier.surrogateId)
      (ClaimKey(s), mkLine(surrogate, s))
    }.groupByKey

  private[emblem] def mkFacility(first: SilverFacilityClaimCohort,
                                 lobMap: Map[String, tables.LineOfBusiness],
                                 diagnoses: List[Diagnosis],
                                 procedures: List[Procedure],
                                 lines: List[Line]): Facility = Facility(
    claimId = Transform.generateUUID(),
    memberIdentifier = MemberIdentifier(
      commonId = first.patient.source.commonId,
      partnerMemberId = first.patient.externalId,
      patientId = first.patient.patientId,
      partner = partner
    ),
    header = Header(
      partnerClaimId = first.claim.CLAIM_ID,
      typeOfBill = first.claim.UB_BILL_TYPE,
      admissionType =
        first.claim.ADM_TYPE.flatMap(Conversions.safeParse(_, _.toInt).map(_.toString)),
      admissionSource = first.claim.ADM_SRC,
      dischargeStatus = first.claim.DIS_STAT,
      lineOfBusiness = Insurance.FacilityCohortMapping
        .getLineOfBusiness(first.claim, lobMap),
      subLineOfBusiness = Insurance.FacilityCohortMapping
        .getSubLineOfBusiness(first.claim, lobMap),
      drg = DRG(
        version = first.claim.DRGVERSION,
        codeset = None,
        code = first.claim.DRGCODE
      ),
      provider = FacilityClaim.HeaderProvider(
        billing = mkBillingProvider(first),
        referring = mkReferringProvider(first),
        servicing = mkServicingProvider(first),
        operating = None
      ),
      diagnoses = Transform.uniqueDiagnoses(diagnoses).toList, // TODO add ICD_DIAG_01 as principal
      procedures = Transform.uniqueProcedures(procedures).toList, // TODO add ICD_PROC_01 as principal
      date = mkDate(first)
    ),
    lines = lines
  )

  private[emblem] def mkBillingProvider(
    first: SilverFacilityClaimCohort): Option[ProviderIdentifier] =
    for (id <- first.claim.BILL_PROV)
      yield
        ProviderIdentifier(
          id = ProviderKey(id, first.claim.BILLINGPROVLOCATIONSUFFIX).uuid.toString,
          specialty = None
        )

  private[emblem] def mkServicingProvider(
    first: SilverFacilityClaimCohort): Option[ProviderIdentifier] =
    for (id <- first.claim.ATT_PROV)
      yield
        ProviderIdentifier(
          id = ProviderKey(id, first.claim.SERVICINGPROVLOCSUFFIX).uuid.toString,
          specialty = first.claim.ATT_PROV_SPEC
        )

  private[emblem] def mkReferringProvider(
    first: SilverFacilityClaimCohort): Option[ProviderIdentifier] =
    for (id <- first.claim.REF_PROV)
      yield
        ProviderIdentifier(
          id = ProviderKey(id, first.claim.REFERRINGPROVLOCATIONSUFFIX).uuid.toString,
          specialty = None
        )

  private[emblem] def mkDate(first: SilverFacilityClaimCohort): Date = Date(
    from = first.claim.FROM_DATE,
    to = first.claim.TO_DATE,
    admit = first.claim.ADM_DATE,
    discharge = first.claim.DIS_DATE,
    paid = first.claim.PAID_DATE
  )

  private[emblem] def mkFirstLineDiagnosis(surrogate: Surrogate,
                                           first: SilverFacilityClaimCohort): Option[Diagnosis] =
    first.claim.ICD_DIAG_01.map { code =>
      Diagnosis(
        surrogate = surrogate,
        tier = DiagnosisTier.Principal.name,
        codeset = CodeSet.ICD10Cm.toString,
        code = code
      )
    }

  private[emblem] def mkFirstLineProcedure(
    surrogate: Surrogate,
    first: SilverFacilityClaimCohort
  ): Option[Procedure] =
    first.claim.ICD_PROC_01.map { code =>
      Procedure(
        surrogate = surrogate,
        tier = ProcedureTier.Principal.name,
        codeset = CodeSet.ICD10Pcs.toString,
        code = code,
        modifiers = List()
      )
    }

  private[emblem] def mkLine(surrogate: Surrogate, silver: SilverFacilityClaimCohort): Line = Line(
    surrogate = surrogate,
    lineNumber = silver.claim.SV_LINE.getOrElse(0),
    revenueCode = silver.claim.REV_CODE,
    cobFlag = cobFlag(silver.claim.COBINDICATOR),
    capitatedFlag = capitatedFlag(silver.claim.FFSCAPIND),
    claimLineStatus = silver.claim.SV_STAT.flatMap(claimLineStatus),
    inNetworkFlag = silver.claim.CLAIM_IN_NETWORK,
    serviceQuantity = silver.claim.SV_UNITS,
    typesOfService = silver.claim.TOS.map(TypeOfService(1, _)).toList,
    procedure = mkProcedure(surrogate, silver),
    amount = mkAmount(silver)
  )

  private[emblem] def mkProcedure(surrogate: Surrogate,
                                  silver: SilverFacilityClaimCohort): Option[Procedure] =
    Transform.mkProcedure(
      surrogate,
      silver,
      ProcedureTier.Secondary,
      (f: SilverFacilityClaimCohort) => f.claim.PROC_CODE,
      (f: SilverFacilityClaimCohort) => (f.claim.CPT_MOD_1 ++ f.claim.CPT_MOD_2).toList
    )

  private[emblem] def mkAmount(silver: SilverFacilityClaimCohort): Amount =
    Constructors.mkAmount(
      allowed = silver.claim.AMT_ALLOWED,
      billed = silver.claim.AMT_BILLED,
      COB = silver.claim.AMT_COB,
      copay = silver.claim.AMT_COPAY,
      deductible = silver.claim.AMT_DEDUCT,
      coinsurance = silver.claim.AMT_COINS,
      planPaid = silver.claim.AMT_PAID
    )
}
