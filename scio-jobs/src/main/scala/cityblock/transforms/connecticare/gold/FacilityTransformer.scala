package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.Medical
import cityblock.models.Surrogate
import cityblock.models.gold.Claims._
import cityblock.models.gold.FacilityClaim
import cityblock.models.gold.FacilityClaim._
import cityblock.transforms.Transform
import cityblock.transforms.Transform.Transformer
import cityblock.transforms.connecticare.AmisysClaimKey
import cityblock.utilities.Conversions
import cityblock.utilities.connecticare.Insurance
import com.spotify.scio.util.MultiJoin
import com.spotify.scio.values.SCollection

case class FacilityTransformer(diagnosisIndexCom: SCollection[IndexedDiagnoses],
                               procedureIndexCom: SCollection[IndexedProcedures],
                               commercial: SCollection[Medical])
    extends Transformer[Facility] {
  override def transform()(implicit pr: String): SCollection[Facility] =
    FacilityTransformer.pipeline(pr, "Medical", diagnosisIndexCom, procedureIndexCom, commercial)
}

object FacilityTransformer extends MedicalMapping {

  /**
   * Transforms a collection of silver facility claim lines into gold [[Facility]] claims.
   *
   * @param project        source project for `silver`
   * @param table          source table for `silver`
   * @param diagnosisIndex gold [[Diagnosis]] indexed by [[ClaimKey]]
   * @param procedureIndex gold [[Procedure]] indexed by [[ClaimKey]]
   * @param silver         silver facility claim lines
   * @return gold [[Facility]] claims
   */
  private def pipeline(project: String,
                       table: String,
                       diagnosisIndex: SCollection[IndexedDiagnoses],
                       procedureIndex: SCollection[IndexedProcedures],
                       silver: SCollection[Medical]): SCollection[Facility] =
    MultiJoin
      .left(
        silver.groupBy(ClaimKey(_)),
        diagnosisIndex,
        procedureIndex,
        linesByClaimKey(project, table, silver)
      )
      .flatMap {
        case (_, (silverLines, diagnoses, procedures, Some(lines))) =>
          Some(
            mkFacility(silverLines.toList,
                       diagnoses.getOrElse(Iterable()).toList,
                       procedures.getOrElse(Iterable()).toList,
                       lines.toList))
        case _ => None
      }

  /**
   * Constructs gold facility [[Line]]s and groups them by [[ClaimKey]].
   *
   * @param project source project for `silver`
   * @param table   source table for `silver`
   * @param silver  silver facility claim lines
   * @return gold [[Line]]s indexed by [[ClaimKey]]
   */
  private def linesByClaimKey(
    project: String,
    table: String,
    silver: SCollection[Medical]): SCollection[(ClaimKey, Iterable[Line])] =
    silver.flatMap { s =>
      val (surrogate, _) =
        Transform.addSurrogate(project, "silver_claims", table, s)(_.identifier.surrogateId)
      for (line <- mkLine(surrogate, s)) yield (ClaimKey(s), line)
    }.groupByKey

  private[connecticare] def mkFacility(silverLines: List[Medical],
                                       diagnoses: List[Diagnosis],
                                       procedures: List[Procedure],
                                       lines: List[Line]): Facility = {
    val first = silverLines.min(CCILineOrdering)

    Facility(
      claimId = AmisysClaimKey(first).uuid.toString,
      memberIdentifier = MemberIdentifier(
        patientId = first.patient.patientId,
        partnerMemberId = first.patient.externalId,
        commonId = first.patient.source.commonId,
        partner = partner
      ),
      header = Header(
        partnerClaimId = first.medical.ClmNum,
        typeOfBill = TypeOfBill.get(silverLines),
        admissionType = first.medical.AdmitType
          .flatMap(Conversions.safeParse(_, _.toInt))
          .map(_.toString),
        admissionSource = None,
        dischargeStatus = first.medical.DischargeStatus,
        lineOfBusiness = Insurance.MedicalMapping.getLineOfBusiness(first).map(_.toString),
        subLineOfBusiness = Insurance.MedicalMapping.getSubLineOfBusiness(first).map(_.toString),
        drg = DRG(
          version = None,
          codeset = None,
          code = first.medical.DRG
        ),
        provider = FacilityClaim.HeaderProvider(
          billing = mkBillingProvider(first),
          referring = mkReferringProvider(first),
          servicing = mkServicingProvider(first),
          operating = None
        ),
        diagnoses = Transform.uniqueDiagnoses(diagnoses).toList,
        procedures = Transform.uniqueProcedures(procedures).toList,
        date = mkDate(first),
      ),
      lines = lines
    )
  }

  /**
   * Maps the [[Date]]s for this facility claim.
   * @param first silver facility claim
   * @return initialized [[Date]]
   */
  private[connecticare] def mkDate(first: Medical): Date = Date(
    from = first.medical.DateEff,
    to = first.medical.DateEnd,
    admit = first.medical.DateAdmit,
    discharge = first.medical.DateDischarge,
    paid = first.medical.DatePaid
  )

  /**
   * Constructs a gold [[Line]].
   *
   * @param surrogate silver_claims reference
   * @param silver silver facility claim line
   * @return a gold [[Line]] or [[None]] if the line doesn't have a valid line number
   */
  private[connecticare] def mkLine(surrogate: Surrogate, silver: Medical): Option[Line] =
    for (num <- lineNumber(silver))
      yield
        Line(
          surrogate = surrogate,
          lineNumber = num,
          revenueCode = revenueCode(silver),
          cobFlag = Some(cobFlag(silver)),
          capitatedFlag = isCapitated(silver.medical.COCLevel2),
          claimLineStatus = Some(ClaimLineStatus.Paid.toString),
          inNetworkFlag = inNetworkFlag(silver),
          serviceQuantity = serviceQuantity(silver),
          typesOfService = List(),
          procedure = chooseFirstProcedure(surrogate, silver),
          amount = mkAmount(silver)
        )

  /**
   * Finds the revenue code (if there is one) in `Proc1` and `Proc2`. The code in `Proc1` is
   * chosen if there are two revenue codes.
   *
   * @param silver silver facility claim line
   * @return the claim line's primary revenue code
   */
  private[connecticare] def revenueCode(silver: Medical): Option[String] =
    procCodesAndTypes(silver).flatMap {
      case (proc, procType) if procType.contains(ProcType.REV.toString) => proc
      case _                                                            => None
    }.headOption
}
