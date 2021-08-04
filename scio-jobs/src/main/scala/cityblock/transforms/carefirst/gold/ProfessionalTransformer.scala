package cityblock.transforms.carefirst.gold

import cityblock.models.CarefirstSilverClaims.{SilverProfessional, SilverProvider}
import cityblock.models.Surrogate
import cityblock.models.gold.Claims._
import cityblock.models.gold.ProfessionalClaim._
import cityblock.models.gold._
import cityblock.models.gold.enums.{DiagnosisTier, ProcedureTier}
import cityblock.transforms.Transform
import cityblock.transforms.Transform.CodeSet.CodeSet
import cityblock.transforms.Transform.{CodeSet, Transformer}
import cityblock.utilities.Conversions
import cityblock.utilities.Insurance.LineOfBusiness
import com.spotify.scio.util.MultiJoin
import com.spotify.scio.values.SCollection

case class ProfessionalTransformer(
  professional: SCollection[SilverProfessional],
  diagnosisIndex: SCollection[(ClaimKey, Iterable[Diagnosis])],
  provider: SCollection[SilverProvider],
) extends Transformer[Professional] {

  override def transform()(implicit pr: String): SCollection[Professional] =
    ProfessionalCohortTransformer.pipeline(pr, professional, diagnosisIndex, provider)
}

object ProfessionalCohortTransformer
    extends MedicalMapping
    with CarefirstLineOrdering[SilverProfessional] {
  override def lineNumber(silver: SilverProfessional): Option[Int] =
    silver.data.LINE_NUM.flatMap(Conversions.safeParse(_, _.toInt))

  object CarefirstLineAndProviderOrdering
      extends Ordering[(SilverProfessional, Option[SilverProvider])] {
    override def compare(
      x: (SilverProfessional, Option[SilverProvider]),
      y: (SilverProfessional, Option[SilverProvider])
    ): Int = Transform.ClaimLineOrdering.compare(lineNumber(x._1), lineNumber(y._1))
  }

  /**
   * Sets up the pipeline to run this
   * @param project source project for Carefirst silver claims
   * @param professional Carefirst professional claims
   * @param diagnosisIndex gold diagnoses indexed by [[ClaimKey]]
   * @param provider most recent Carefirst provider shard
   * @return gold [[Professional]] claims
   */
  def pipeline(
    project: String,
    professional: SCollection[SilverProfessional],
    diagnosisIndex: SCollection[(ClaimKey, Iterable[Diagnosis])],
    provider: SCollection[SilverProvider],
  ): SCollection[Professional] = {
    val joinedWithProvider: SCollection[(SilverProfessional, Option[SilverProvider])] =
      professional
        .keyBy(_.data.REF_PROV_ID)
        .leftOuterJoin(provider.keyBy(prov => Some(prov.data.PROV_ID)))
        .values
    val firstsByClaimKey: SCollection[(ClaimKey, (SilverProfessional, Option[SilverProvider]))] =
      firstSilverLinesByClaimKey(joinedWithProvider)

    MultiJoin
      .left(
        firstsByClaimKey,
        diagnosisIndex,
        linesByClaimKey(project, joinedWithProvider)
      )
      .flatMap {
        case (claimKey, (first, dxs, Some(lines))) =>
          Some(
            mkProfessional(
              project,
              claimKey,
              first._1,
              dxs.getOrElse(Iterable()).toList,
              lines.toList,
              first._2
            )
          )
        case _ => None
      }
  }

  /**
   * Selects lowest-numbered line of each claim line and indexes by [[ClaimKey]].
   * @param professional Emblem professional claim lines
   * @return first lines of claims indexed by [[ClaimKey]]
   */
  private def firstSilverLinesByClaimKey(
    professional: SCollection[(SilverProfessional, Option[SilverProvider])]
  ): SCollection[(ClaimKey, (SilverProfessional, Option[SilverProvider]))] =
    professional
      .keyBy(prof => ClaimKey(prof._1))
      .reduceByKey(CarefirstLineAndProviderOrdering.min)

  /**
   * Constructs gold professional [[Line]]s and groups them by [[ClaimKey]].
   * @param project source project of silver claim lines
   * @param professional silver professional claim lines
   * @return gold professional [[Line]]s grouped by [[ClaimKey]]
   */
  private def linesByClaimKey(
    project: String,
    professional: SCollection[(SilverProfessional, Option[SilverProvider])]
  ): SCollection[(ClaimKey, Iterable[Line])] =
    professional
      .map {
        case (silver, provider) =>
          val (surrogate, _) =
            Transform.addSurrogate(project, "silver_claims", "professional", silver)(
              _.identifier.surrogateId)
          (ClaimKey(silver), mkLine(surrogate, silver, provider))
      }
      .groupByKey
      .map(grouped => (grouped._1, grouped._2.flatten))

  private def mkLine(surrogate: Surrogate,
                     silver: SilverProfessional,
                     provider: Option[SilverProvider]): Option[Line] =
    for {
      lineNumber <- lineNumber(silver)
    } yield {
      Line(
        surrogate = surrogate,
        lineNumber = lineNumber,
        cobFlag = silver.data.COB_IND,
        capitatedFlag = silver.data.CAP_IND,
        claimLineStatus = silver.data._claim_line_status.map(claimLineStatus),
        inNetworkFlag = silver.data.CLAIM_IN_NETWORK.flatMap(Conversions.safeParse(_, _.toBoolean)),
        serviceQuantity = silver.data.SV_UNITS.flatMap(Conversions.safeParse(_, _.toInt)),
        placeOfService = silver.data.POS,
        date = mkDate(silver),
        provider = LineProvider(
          servicing = mkProvider(silver.data.REF_PROV_ID, provider)
        ),
        procedure = mkProcedure(surrogate, silver),
        amount = mkAmount(silver),
        diagnoses = mkLineDiagnoses(surrogate, silver),
        typesOfService = List.empty
      )
    }

  private def mkDate(silver: SilverProfessional): ProfessionalDate =
    ProfessionalDate(
      from = silver.data.FROM_DATE,
      to = silver.data.TO_DATE,
      paid = silver.data.PAID_DATE
    )

  private def mkProcedure(surrogate: Surrogate, silver: SilverProfessional): Option[Procedure] = {
    val tier = if (isFirstLine(silver.data.LINE_NUM)) {
      ProcedureTier.Principal
    } else {
      ProcedureTier.Secondary
    }

    Transform.mkProcedure(
      surrogate,
      silver,
      tier,
      (p: SilverProfessional) => p.data.PROC_CODE,
      (p: SilverProfessional) => (p.data.CPT_MOD_1 ++ p.data.CPT_MOD_2).toList
    )
  }

  private def mkAmount(silver: SilverProfessional): Amount =
    Constructors.mkAmount(
      allowed = silver.data.AMT_ALLOWED,
      billed = silver.data.AMT_BILLED,
      COB = silver.data.AMT_COB,
      copay = silver.data.AMT_COPAY,
      deductible = silver.data.AMT_DEDUCT,
      coinsurance = silver.data.AMT_COINS,
      planPaid = silver.data.AMT_PAID
    )

  private def mkLineDiagnoses(surrogate: Surrogate, silver: SilverProfessional): List[Diagnosis] = {
    val tier = if (isFirstLine(silver.data.LINE_NUM)) {
      ProcedureTier.Principal
    } else {
      ProcedureTier.Secondary
    }

    silver.data.ICD_DIAG_01.map { code =>
      Diagnosis(
        surrogate = surrogate,
        tier = tier.name,
        code = cleanDiagnosisCode(code),
        codeset = getICDCodeset(silver).toString
      )
    }.toList
  }

  private def getICDCodeset(ICD10_OR_HIGHER: Boolean): CodeSet =
    if (ICD10_OR_HIGHER) CodeSet.ICD10Cm else CodeSet.ICD9Cm

  def getICDCodeset(silver: SilverProfessional): CodeSet =
    silver.data.ICD10_OR_HIGHER.fold(CodeSet.ICD9Cm)(getICDCodeset)

  /**
   * Constructs a gold [[Professional]] claim.
   * @param first the first line of the Emblem silver professional claim
   * @param diagnoses claim-level diagnoses
   * @param lines gold claim lines
   * @return gold professional claim
   */
  private def mkProfessional(
    project: String,
    claimKey: ClaimKey,
    first: SilverProfessional,
    diagnoses: List[Diagnosis],
    lines: List[Line],
    provider: Option[SilverProvider]
  ): Professional =
    Professional(
      claimId = claimKey.uuid.toString,
      memberIdentifier = MemberIdentifier(
        patientId = first.patient.patientId,
        partnerMemberId = first.patient.externalId,
        commonId = first.patient.source.commonId,
        partner = partner
      ),
      header = mkHeader(project, first, diagnoses, provider),
      lines = lines
    )

  private def mkHeader(
    project: String,
    first: SilverProfessional,
    diagnoses: Iterable[Diagnosis],
    provider: Option[SilverProvider]
  ): Header =
    Header(
      partnerClaimId = first.data.CLAIM_NUM,
      lineOfBusiness = Some(LineOfBusiness.Medicaid.toString),
      subLineOfBusiness = Some("Supplemental Security Income"),
      provider = HeaderProvider(
        billing = mkProvider(first.data.BILL_PROV_ID, provider),
        referring = mkProvider(first.data.REF_PROV_ID, provider)
      ),
      diagnoses = mkHeaderDiagnoses(project, diagnoses, first)
    )

  private def mkHeaderDiagnoses(project: String,
                                diagnoses: Iterable[Diagnosis],
                                first: SilverProfessional): List[Diagnosis] = {
    val silverSurrogate =
      Surrogate(first.identifier.surrogateId, project, "silver_claims", "professional")
    val combinedDiagnoses = (diagnoses ++ mkFirstLineDiagnosis(silverSurrogate, first))
      .map(diagnosis => diagnosis.copy(code = cleanDiagnosisCode(diagnosis.code)))

    Transform
      .uniqueDiagnoses(combinedDiagnoses)
      .toList
  }

  private def mkFirstLineDiagnosis(surrogate: Surrogate,
                                   first: SilverProfessional): Option[Diagnosis] =
    first.data.ICD_DIAG_01.map { code =>
      Diagnosis(
        surrogate = surrogate,
        tier = DiagnosisTier.Principal.name,
        codeset = CodeSet.ICD10Cm.toString,
        code = cleanDiagnosisCode(code)
      )
    }

  private def mkProvider(
    providerId: Option[String],
    provider: Option[SilverProvider]
  ): Option[ProviderIdentifier] =
    for (id <- providerId)
      yield
        ProviderIdentifier(
          id = ProviderKey(id).uuid.toString,
          specialty = provider.flatMap(_.data.PROV_SPEC_CODE)
        )
}
