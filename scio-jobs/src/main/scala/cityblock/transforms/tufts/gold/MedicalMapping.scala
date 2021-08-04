package cityblock.transforms.tufts.gold

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.models.Surrogate
import cityblock.models.TuftsSilverClaims.Medical
import cityblock.models.gold.Claims.{ClaimLineStatus, Diagnosis, MemberIdentifier, Procedure}
import cityblock.models.gold.enums.{DiagnosisTier, ProcedureTier}
import cityblock.models.gold.{Amount, Constructors, ProviderIdentifier}
import cityblock.models.tufts.silver.Medical.ParsedMedical
import cityblock.transforms.Transform
import cityblock.transforms.Transform.CodeSet
import cityblock.transforms.tufts.{ClaimKey, ProviderKey}
import cityblock.utilities.Conversions
import cityblock.utilities.reference.tables.ProviderSpecialtyMappings
import com.spotify.scio.values.SCollection

trait MedicalMapping {
  def firstByClaimKey(
    lines: SCollection[LineWithSpecialties]
  ): SCollection[(ClaimKey, LineWithSpecialties)] =
    lines
      .keyBy(claim => ClaimKey(claim.claimLine))
      .reduceByKey(TuftsLineOrdering.min)

  def mkCaseClass(
    lines: SCollection[((Surrogate, Medical), Option[ProviderSpecialtyMappings])]
  ): SCollection[LineWithSpecialties] =
    lines.map {
      case ((surrogate, claimLine), maybeSpecialty) =>
        LineWithSpecialties(
          surrogate = surrogate,
          claimLine = claimLine,
          lineSpecialty = maybeSpecialty
        )
    }

  def linesWithLineSpecialty(
    claimsWithSurrogates: SCollection[(Surrogate, Medical)],
    providerSpecialties: SCollection[ProviderSpecialtyMappings]
  ): SCollection[((Surrogate, Medical), Option[ProviderSpecialtyMappings])] =
    claimsWithSurrogates
      .keyBy { case (surrogate, medical) => medical.data.Service_Provider_Taxonomy }
      .leftOuterJoin(providerSpecialties.keyBy(mapping => Some(mapping.code)))
      .values

  def convertAmount(amount: Option[String]): Option[String] =
    amount.flatMap(Conversions.safeParse(_, _.toFloat)).map(_ / 100.00).map(_.toString)

  def mkAmount(line: ParsedMedical): Amount =
    Constructors.mkAmount(
      allowed = convertAmount(line.Allowed_amount),
      billed = convertAmount(line.Charge_Amount),
      COB = convertAmount(line.Coordination_of_Benefits_TPL_Liability_Amount),
      copay = convertAmount(line.Copay_Amount),
      deductible = convertAmount(line.Deductible_Amount),
      coinsurance = convertAmount(line.Coinsurance_Amount),
      planPaid = convertAmount(line.Paid_Amount)
    )

  def mkCapitatedFlag(line: ParsedMedical): Option[Boolean] =
    line.Capitated_Encounter_Flag.map(_ == "1")

  def mkClaimLineStatus(line: ParsedMedical): Option[String] =
    line.Claim_Status.flatMap(_ match {
      case "1" => Some(ClaimLineStatus.Paid.toString)
      case "2" => Some(ClaimLineStatus.Paid.toString)
      case "4" => Some(ClaimLineStatus.Denied.toString)
      case "?" => Some(ClaimLineStatus.Denied.toString)
      case _   => None
    })

  def mkInNetworkFlag(line: ParsedMedical): Option[Boolean] =
    line.InNetwork_Indicator.map(_ == "1")

  def mkServiceQuantity(line: ParsedMedical): Option[Int] =
    line.Quantity.flatMap(Conversions.safeParse(_, _.toInt))

  def mkCobFlag(line: ParsedMedical): Option[Boolean] =
    line.Coordination_of_Benefits_TPL_Liability_Amount
      .flatMap(Conversions.safeParse(_, _.toInt))
      .map(_ > 0)

  def mkProcedure(line: ParsedMedical, surrogate: Surrogate): Option[Procedure] =
    line.Procedure_Code.map(code => {
      val codeset: String = Transform.determineProcedureCodeset(code)

      Procedure(
        surrogate = surrogate,
        tier = ProcedureTier.Secondary.name,
        codeset = codeset,
        code = code,
        modifiers = List(
          line.Procedure_Modifier_1,
          line.Procedure_Modifier_2,
          line.Procedure_Modifier_3,
          line.Procedure_Modifier_4
        ).flatten
      )
    })

  private def mkSecondaryDiagnosis(diagnosisCode: String, surrogate: Surrogate): Diagnosis =
    Diagnosis(
      surrogate = surrogate,
      tier = DiagnosisTier.Secondary.name,
      codeset = CodeSet.ICD10Cm.toString,
      code = diagnosisCode
    )

  def mkDiagnoses(line: ParsedMedical, surrogate: Surrogate): List[Diagnosis] = {
    val secondaryDiagnosisCodes = List(
      line.Other_Diagnosis_1,
      line.Other_Diagnosis_2,
      line.Other_Diagnosis_3,
      line.Other_Diagnosis_4,
      line.Other_Diagnosis_5,
      line.Other_Diagnosis_6,
      line.Other_Diagnosis_7,
      line.Other_Diagnosis_8,
      line.Other_Diagnosis_9,
      line.Other_Diagnosis_10,
      line.Other_Diagnosis_11,
      line.Other_Diagnosis_12
    ).flatten

    val secondaryDiagnoses: List[Diagnosis] =
      secondaryDiagnosisCodes.map(mkSecondaryDiagnosis(_, surrogate))

    val admitDiagnosis: List[Diagnosis] = line.Admitting_Diagnosis
      .map(code => {
        Diagnosis(
          surrogate = surrogate,
          tier = DiagnosisTier.Admit.name,
          codeset = CodeSet.ICD10Cm.toString,
          code = code
        )
      })
      .toList

    val principalDiagnosis: List[Diagnosis] = line.Principal_Diagnosis
      .map(code => {
        Diagnosis(
          surrogate = surrogate,
          tier = DiagnosisTier.Principal.name,
          codeset = CodeSet.ICD10Cm.toString,
          code = code
        )
      })
      .toList

    secondaryDiagnoses ++ admitDiagnosis ++ principalDiagnosis
  }

  def mkClaimId(claim: Medical): String = ClaimKey(claim).uuid.toString

  def mkProviderIdentifier(providerId: Option[String]): Option[ProviderIdentifier] =
    providerId.map(id => {
      ProviderIdentifier(
        id = ProviderKey(id).uuid.toString,
        specialty = None
      )
    })

  def mkMemberIdentifier(claim: Medical): MemberIdentifier = {
    val patient: Patient = claim.patient
    MemberIdentifier(
      patientId = patient.patientId,
      partnerMemberId = claim.data.CarrierSpecificUniqueMemberID,
      commonId = patient.source.commonId,
      partner = partner
    )
  }

  def getVersionNumber(line: ParsedMedical): Option[Int] =
    line.Version_Number.flatMap(Conversions.safeParse(_, _.toInt))

  def addSurrogate(
    project: String,
    table: String,
    claims: SCollection[Medical]
  ): SCollection[(Surrogate, Medical)] =
    claims.map(
      claim =>
        Transform.addSurrogate(project, "silver_claims", table, claim)(_.identifier.surrogateId)
    )

  case class LineWithSpecialties(
    surrogate: Surrogate,
    claimLine: Medical,
    lineSpecialty: Option[ProviderSpecialtyMappings]
  )

  private def getVersionNumberForSort(versionNumber: Option[String]): Int =
    versionNumber.flatMap(Conversions.safeParse(_, _.toInt)).map(_ * -1).getOrElse(Int.MaxValue)

  implicit val TuftsLineOrdering = new Ordering[LineWithSpecialties] {
    def compare(self: LineWithSpecialties, that: LineWithSpecialties): Int =
      self.claimLine.data.Line_Counter compare that.claimLine.data.Line_Counter match {
        case 0 =>
          getVersionNumberForSort(self.claimLine.data.Version_Number) compare getVersionNumberForSort(
            that.claimLine.data.Version_Number
          )
        case c => c
      }
  }
}
