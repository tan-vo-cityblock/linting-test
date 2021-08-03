package cityblock.transforms.carefirst.gold

import cityblock.models.CarefirstSilverClaims.{
  SilverDiagnosisAssociation,
  SilverFacility,
  SilverPharmacy,
  SilverProcedureAssociation,
  SilverProfessional
}
import cityblock.transforms.Transform.{Key, UUIDComparable}

sealed case class ClaimKey(claimNumber: String) extends Key with UUIDComparable {
  override protected val elements: List[String] = List(partner, claimNumber)
}
object ClaimKey {
  def apply(medical: SilverProfessional): ClaimKey =
    ClaimKey(medical.data.CLAIM_NUM)
  def apply(diagnosis: SilverDiagnosisAssociation): ClaimKey =
    ClaimKey(diagnosis.data.CLAIM_NUM)
  def apply(procedure: SilverProcedureAssociation): ClaimKey =
    ClaimKey(procedure.data.CLAIM_NUM)
  def apply(facility: SilverFacility): ClaimKey =
    ClaimKey(facility.data.CLAIM_NUM)
  def apply(pharmacy: SilverPharmacy): ClaimKey =
    ClaimKey(pharmacy.data.CLAIM_NUM)
}
