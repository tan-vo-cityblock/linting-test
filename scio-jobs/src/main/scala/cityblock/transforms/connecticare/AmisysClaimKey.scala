package cityblock.transforms.connecticare

import cityblock.models.ConnecticareSilverClaims.{Medical, UBH}
import cityblock.transforms.Transform.{Key, UUIDComparable}
import cityblock.transforms.tufts.gold.partner

sealed case class AmisysClaimKey(memberId: String, claimNumber: String)
    extends Key
    with UUIDComparable {
  override protected val elements: List[String] = List(partner, memberId, claimNumber)
}
object AmisysClaimKey {
  def apply(medical: Medical): AmisysClaimKey =
    AmisysClaimKey(medical.patient.externalId, medical.medical.ClmNum)
  def apply(behavioral: UBH): AmisysClaimKey =
    AmisysClaimKey(behavioral.patient.externalId, behavioral.claim.AUDNBR)
}
