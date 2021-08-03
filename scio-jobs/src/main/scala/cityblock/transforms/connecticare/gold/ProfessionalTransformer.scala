package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.Medical
import cityblock.models.Surrogate
import cityblock.models.gold.Claims._
import cityblock.models.gold.ProfessionalClaim._
import cityblock.transforms.Transform
import cityblock.transforms.Transform.Transformer
import cityblock.transforms.connecticare.AmisysClaimKey
import cityblock.utilities.connecticare.Insurance
import com.spotify.scio.util.MultiJoin
import com.spotify.scio.values.SCollection

/**
 * Transforms CCI silver professional [[MedicalMapping]] claims and [[Indices]] into gold [[Professional]]
 * claims.
 *
 * @param diagnosisIndexCom gold commercial diagnoses indexed by [[ClaimKey]]
 * @param commercial commercial silver professional claims
 */
case class ProfessionalTransformer(
  diagnosisIndexCom: SCollection[IndexedDiagnoses],
  commercial: SCollection[Medical]
) extends Transformer[Professional] {

  /**
   * Executes the transform.
   * @param pr source project for the collections given as parameters to this [[Transformer]]
   * @return gold [[Professional]] claims
   */
  override def transform()(implicit pr: String): SCollection[Professional] =
    ProfessionalTransformer.pipeline(pr, "Medical", commercial, diagnosisIndexCom)
}

/**
 * Pipeline definition, constructors, and field mappings for transforming CCI [[MedicalMapping]] claims
 * into gold Professional claims.
 */
object ProfessionalTransformer extends MedicalMapping {

  /**
   * Transforms either commercial or Medicare CCI silver professional claims into gold professional
   * claims.
   *
   * @param project             source project (for constructing [[Surrogate]])
   * @param table               source table (for constructing [[Surrogate]])
   * @param silver              silver professional claims
   * @param diagnosisIndex      gold diagnoses indexed by [[ClaimKey]]
   * @return gold [[Professional]] claims
   */
  private def pipeline(project: String,
                       table: String,
                       silver: SCollection[Medical],
                       diagnosisIndex: SCollection[IndexedDiagnoses]): SCollection[Professional] = {
    val firstsByClaimKey = silver
      .keyBy(ClaimKey(_))
      .reduceByKey(CCILineOrdering.min)

    MultiJoin
      .left(
        firstsByClaimKey,
        diagnosisIndex,
        linesByClaimKey(project, table, silver)
      )
      .flatMap {
        case (_, (first, diagnoses, Some(lines))) =>
          Some(mkProfessional(first, diagnoses.getOrElse(Iterable()).toList, lines.toList))
        case _ => None
      }
  }

  /**
   * Constructs gold professional [[Line]]s and indexes them by [[ClaimKey]].
   * @param project source project for `silver`
   * @param table source table for `silver`
   * @param silver silver professional claim lines
   * @return gold [[Line]]s indexed by [[ClaimKey]]
   */
  private def linesByClaimKey(
    project: String,
    table: String,
    silver: SCollection[Medical]): SCollection[(ClaimKey, Iterable[Line])] =
    silver.flatMap { s =>
      val (surrogate, _) =
        Transform.addSurrogate(project, "silver_claims", table, s)(_.identifier.surrogateId)
      for (line <- mkLine(surrogate, s))
        yield (ClaimKey(s), line)
    }.groupByKey

  /**
   * Constructs a gold professional [[Line]].
   *
   * Returns [[None]] if the claim's line number cannot be parsed as an [[Int]].
   *
   * @param surrogate BigQuery reference to the associated silver line
   * @param silver silver professional line
   * @return gold professional [[Line]]
   */
  private[connecticare] def mkLine(surrogate: Surrogate, silver: Medical): Option[Line] =
    for (num <- lineNumber(silver))
      yield
        Line(
          surrogate = surrogate,
          lineNumber = num,
          cobFlag = Some(cobFlag(silver)),
          capitatedFlag = isCapitated(silver.medical.COCLevel2),
          claimLineStatus = Some(ClaimLineStatus.Paid.toString),
          inNetworkFlag = inNetworkFlag(silver),
          serviceQuantity = serviceQuantity(silver),
          placeOfService = silver.medical.Location,
          date = mkDate(silver),
          provider = LineProvider(
            servicing = mkServicingProvider(silver)
          ),
          procedure = chooseFirstProcedure(surrogate, silver),
          amount = mkAmount(silver),
          diagnoses = List(),
          typesOfService = List()
        )

  private[connecticare] def mkDate(silver: Medical): ProfessionalDate = ProfessionalDate(
    from = silver.medical.DateEff,
    to = silver.medical.DateEnd,
    paid = silver.medical.DatePaid
  )

  /**
   * Constructs a gold [[Professional]] claim.
   * @param first first line of the claim
   * @param diagnoses claim-level diagnoses
   * @param lines gold professional [[Line]]s
   * @return gold [[Professional]] claim
   */
  private[connecticare] def mkProfessional(first: Medical,
                                           diagnoses: List[Diagnosis],
                                           lines: List[Line]): Professional =
    Professional(
      claimId = AmisysClaimKey(first).uuid.toString,
      memberIdentifier = MemberIdentifier(
        patientId = first.patient.patientId,
        partnerMemberId = first.patient.externalId,
        commonId = first.patient.source.commonId,
        partner = partner
      ),
      header = mkHeader(first, Transform.uniqueDiagnoses(diagnoses, lines)),
      lines = lines
    )

  private[connecticare] def mkHeader(first: Medical, diagnoses: Iterable[Diagnosis]): Header =
    Header(
      partnerClaimId = first.medical.ClmNum,
      lineOfBusiness = Insurance.MedicalMapping.getLineOfBusiness(first).map(_.toString),
      subLineOfBusiness = Insurance.MedicalMapping.getSubLineOfBusiness(first).map(_.toString),
      provider = HeaderProvider(
        billing = mkBillingProvider(first),
        referring = mkReferringProvider(first)
      ),
      diagnoses = Transform.uniqueDiagnoses(diagnoses).toList
    )
}
