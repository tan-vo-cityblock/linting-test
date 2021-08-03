package cityblock.transforms.tufts

import cityblock.models.TuftsSilverClaims.{MassHealth, Medical, SilverPharmacy}
import cityblock.transforms.Transform.{Key, UUIDComparable}
import cityblock.transforms.tufts.gold.partner

sealed case class ClaimKey(memberId: String, claimNumber: String) extends Key with UUIDComparable {
  override protected val elements: List[String] = List(partner, memberId, claimNumber)
}
object ClaimKey {
  def apply(medical: Medical): ClaimKey =
    ClaimKey(medical.patient.externalId, medical.data.Payer_Claim_Control_Number)
  def apply(pharmacy: SilverPharmacy): ClaimKey =
    ClaimKey(pharmacy.patient.externalId, pharmacy.data.Script_number)
  def apply(masshealth: MassHealth): ClaimKey =
    ClaimKey(masshealth.patient.externalId, masshealth.data.NUM_LOGICAL_CLAIM)
}
