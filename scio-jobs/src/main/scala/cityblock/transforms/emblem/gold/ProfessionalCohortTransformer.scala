package cityblock.transforms.emblem.gold

import cityblock.models.EmblemSilverClaims.SilverProfessionalClaimCohort
import cityblock.models.Surrogate
import cityblock.models.gold.Claims._
import cityblock.models.gold.ProfessionalClaim._
import cityblock.models.gold._
import cityblock.models.gold.enums.{DiagnosisTier, ProcedureTier}
import cityblock.transforms.Transform
import cityblock.transforms.Transform.Transformer
import cityblock.utilities.emblem.Insurance
import cityblock.utilities.reference.tables
import com.spotify.scio.util.MultiJoin
import com.spotify.scio.values.{SCollection, SideInput}

/**
 * Transforms Emblem [[SilverProfessionalClaimCohort]]s into gold [[Professional]] claims.
 * @param professional Emblem silver professional claims
 * @param diagnosisIndex gold diagnoses indexed by [[ClaimKey]]
 */
case class ProfessionalCohortTransformer(
  professional: SCollection[SilverProfessionalClaimCohort],
  mappedLOB: SideInput[Map[String, tables.LineOfBusiness]],
  diagnosisIndex: SCollection[(ClaimKey, Iterable[Diagnosis])]
) extends Transformer[Professional] {

  /**
   * Runs this transform.
   * @param pr source project for the collections given as parameters to this [[Transformer]]
   * @return gold [[Professional]] claims
   */
  override def transform()(implicit pr: String): SCollection[Professional] =
    ProfessionalCohortTransformer.pipeline(pr, professional, mappedLOB, diagnosisIndex)
}

/**
 * Pipeline definition, constructors, and field mappings for transforming Emblem
 * [[SilverProfessionalClaimCohort]]s into gold [[Professional]] claims.
 */
object ProfessionalCohortTransformer extends EmblemLineOrdering[SilverProfessionalClaimCohort] {
  override protected def svLine(s: SilverProfessionalClaimCohort): Option[Int] =
    s.claim.SV_LINE

  /**
   * Sets up the pipeline to run this
   * @param project source project for Emblem silver claims
   * @param professional Emblem professional claims
   * @param diagnosisIndex gold diagnoses indexed by [[ClaimKey]]
   * @return gold [[Professional]] claims
   */
  private def pipeline(
    project: String,
    professional: SCollection[SilverProfessionalClaimCohort],
    mappedLOB: SideInput[Map[String, tables.LineOfBusiness]],
    diagnosisIndex: SCollection[(ClaimKey, Iterable[Diagnosis])]): SCollection[Professional] = {
    val firstsByClaimKey = firstSilverLinesByClaimKey(professional)

    MultiJoin
      .left(
        firstsByClaimKey,
        diagnosisIndex,
        linesByClaimKey(project, professional)
      )
      .withSideInputs(mappedLOB)
      .flatMap {
        case ((_, (first, dxs, Some(lines))), ctx) =>
          Some(
            mkProfessional(first, ctx(mappedLOB), dxs.getOrElse(Iterable()).toList, lines.toList))
        case _ => None
      }
  }.toSCollection

  /**
   * Selects lowest-numbered line of each claim line and indexes by [[ClaimKey]].
   * @param professional Emblem professional claim lines
   * @return first lines of claims indexed by [[ClaimKey]]
   */
  private def firstSilverLinesByClaimKey(professional: SCollection[SilverProfessionalClaimCohort])
    : SCollection[(ClaimKey, SilverProfessionalClaimCohort)] =
    professional
      .keyBy(ClaimKey(_))
      .reduceByKey(EmblemLineOrdering.min)

  /**
   * Constructs gold professional [[Line]]s and groups them by [[ClaimKey]].
   * @param project source project of silver claim lines
   * @param professional silver professional claim lines
   * @return gold professional [[Line]]s grouped by [[ClaimKey]]
   */
  private def linesByClaimKey(project: String,
                              professional: SCollection[SilverProfessionalClaimCohort])
    : SCollection[(ClaimKey, Iterable[Line])] =
    professional.map { silver =>
      val (surrogate, _) =
        Transform.addSurrogate(project, "silver_claims", "professional", silver)(
          _.identifier.surrogateId)
      (ClaimKey(silver), mkLine(surrogate, silver))
    }.groupByKey

  /**
   * Maps an Emblem [[SilverProfessionalClaimCohort]] line to a gold claim line.
   * @param surrogate reference to the silver claim in `silver_claims`
   * @param silver the silver claim line
   * @return gold claim line
   */
  private[emblem] def mkLine(surrogate: Surrogate, silver: SilverProfessionalClaimCohort): Line =
    Line(
      surrogate = surrogate,
      lineNumber = silver.claim.SV_LINE.getOrElse(0),
      cobFlag = cobFlag(silver.claim.COBINDICATOR),
      capitatedFlag = capitatedFlag(silver.claim.FFSCAPIND),
      claimLineStatus = silver.claim.SV_STAT.flatMap(claimLineStatus),
      inNetworkFlag = silver.claim.CLAIM_IN_NETWORK,
      serviceQuantity = silver.claim.SV_UNITS,
      placeOfService = silver.claim.POS,
      date = mkDate(silver),
      provider = LineProvider(
        servicing = mkServicingProvider(silver)
      ),
      procedure = mkProcedure(surrogate, silver),
      amount = mkAmount(silver),
      diagnoses = mkLineDiagnoses(surrogate, silver),
      typesOfService = silver.claim.TOS.map(TypeOfService(1, _)).toList
    )

  private[emblem] def mkDate(silver: SilverProfessionalClaimCohort): ProfessionalDate =
    ProfessionalDate(
      from = silver.claim.FROM_DATE,
      to = silver.claim.TO_DATE,
      paid = silver.claim.PAID_DATE
    )

  private[emblem] def mkServicingProvider(
    silver: SilverProfessionalClaimCohort): Option[ProviderIdentifier] =
    for (id <- silver.claim.ATT_PROV)
      yield
        ProviderIdentifier(
          id = ProviderKey(id, silver.claim.SERVICINGPROVLOCSUFFIX).uuid.toString,
          specialty = silver.claim.ATT_PROV_SPEC
        )

  private[emblem] def mkProcedure(surrogate: Surrogate,
                                  silver: SilverProfessionalClaimCohort): Option[Procedure] = {
    val tier = if (isFirstLine(silver.claim.SV_LINE)) {
      ProcedureTier.Principal
    } else {
      ProcedureTier.Secondary
    }

    Transform.mkProcedure(
      surrogate,
      silver,
      tier,
      (p: SilverProfessionalClaimCohort) => p.claim.PROC_CODE,
      (p: SilverProfessionalClaimCohort) => (p.claim.CPT_MOD_1 ++ p.claim.CPT_MOD_2).toList
    )
  }

  private[emblem] def mkAmount(silver: SilverProfessionalClaimCohort): Amount =
    Constructors.mkAmount(
      allowed = silver.claim.AMT_ALLOWED,
      billed = silver.claim.AMT_BILLED,
      COB = silver.claim.AMT_COB,
      copay = silver.claim.AMT_COPAY,
      deductible = silver.claim.AMT_DEDUCT,
      coinsurance = silver.claim.AMT_COINS,
      planPaid = silver.claim.AMT_PAID
    )

  /**
   * Creates a gold [[Diagnosis]] for each diagnosis on this [[SilverProfessionalClaimCohort]]
   * line.
   *
   * @note For emblem professional claims the diagnoses from each line are of equal rank, so we
   *       mark them all as principal diagnoses.
   *
   * @param surrogate reference to silver claim line
   * @param silver silver claim line
   * @return list of gold diagnoses from this claim line
   */
  private[emblem] def mkLineDiagnoses(surrogate: Surrogate,
                                      silver: SilverProfessionalClaimCohort): List[Diagnosis] =
    silver.claim.ICD_DIAG_01.map { code =>
      Diagnosis(
        surrogate = surrogate,
        tier = DiagnosisTier.Principal.name,
        code = code,
        codeset = DiagnosisTransformer.getICDCodeset(silver).toString
      )
    }.toList

  /**
   * Constructs a gold [[Professional]] claim.
   * @param first the first line of the Emblem silver professional claim
   * @param diagnoses claim-level diagnoses
   * @param lines gold claim lines
   * @return gold professional claim
   */
  private[emblem] def mkProfessional(first: SilverProfessionalClaimCohort,
                                     lobMap: Map[String, tables.LineOfBusiness],
                                     diagnoses: List[Diagnosis],
                                     lines: List[Line]): Professional =
    Professional(
      claimId = Transform.generateUUID(),
      memberIdentifier = MemberIdentifier(
        patientId = first.patient.patientId,
        partnerMemberId = first.patient.externalId,
        commonId = first.patient.source.commonId,
        partner = partner
      ),
      header = mkHeader(first, lobMap, Transform.uniqueDiagnoses(diagnoses, lines)),
      lines = lines
    )

  private[emblem] def mkHeader(first: SilverProfessionalClaimCohort,
                               lobMap: Map[String, tables.LineOfBusiness],
                               diagnoses: Iterable[Diagnosis]): Header =
    Header(
      partnerClaimId = first.claim.CLAIM_ID,
      lineOfBusiness = Insurance.ProfessionalCohortMapping
        .getLineOfBusiness(first.claim, lobMap),
      subLineOfBusiness = Insurance.ProfessionalCohortMapping
        .getSubLineOfBusiness(first.claim, lobMap),
      provider = HeaderProvider(
        billing = mkBillingProvider(first),
        referring = mkReferringProvider(first)
      ),
      diagnoses = Transform.uniqueDiagnoses(diagnoses).toList
    )

  private[emblem] def mkBillingProvider(
    first: SilverProfessionalClaimCohort): Option[ProviderIdentifier] =
    for (id <- first.claim.BILL_PROV)
      yield
        ProviderIdentifier(
          id = ProviderKey(id, first.claim.BILLINGPROVLOCATIONSUFFIX).uuid.toString,
          specialty = None
        )

  private[emblem] def mkReferringProvider(
    first: SilverProfessionalClaimCohort): Option[ProviderIdentifier] =
    for (id <- first.claim.REF_PROV)
      yield
        ProviderIdentifier(
          id = ProviderKey(id, first.claim.REFERRINGPROVLOCATIONSUFFIX).uuid.toString,
          specialty = None
        )
}
